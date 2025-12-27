import { PGlite, type PGliteInterface } from "@electric-sql/pglite";
import * as assert from "node:assert";
import * as fs from "node:fs/promises";
import { beforeEach, describe, it } from "node:test";

const sqlScript = await fs.readFile(
  new URL("./fs.sql", import.meta.url),
  "utf-8"
);

const dbRoot = new PGlite();
await dbRoot.exec(sqlScript);
let db: PGliteInterface | null = null;

describe("SQL Filesystem with Version Control", () => {
  beforeEach(async () => {
    db = await dbRoot.clone();
  });

  describe("Basic Operations", () => {
    it("should create a repository", async () => {
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>("INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *");
      const repoId = repoResult?.rows[0].id;

      const result = await db?.query<{ name: string }>(
        `SELECT name FROM fs.repositories WHERE id = '${repoId}'`
      );

      assert.strictEqual(result?.rows.length, 1);
      assert.strictEqual(result?.rows[0].name, "test-repo");
    });

    it("should create fs.commits", async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>("INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *");
      const repoId = repoResult?.rows[0].id;

      // Create additional commit
      await db?.exec(`
        INSERT INTO fs.commits (repository_id, parent_commit_id, message)
        VALUES ('${repoId}', (SELECT head_commit_id FROM fs.branches WHERE repository_id = '${repoId}' AND name = 'main'), 'Additional commit')
      `);

      const commitResult = await db?.query<{ message: string }>(
        "SELECT message FROM fs.commits WHERE message = 'Additional commit'"
      );

      assert.strictEqual(commitResult?.rows.length, 1);
      assert.strictEqual(commitResult?.rows[0].message, "Additional commit");
    });

    it("should write and read files", async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>("INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *");
      const repoId = repoResult?.rows[0].id;

      await db?.exec(`
        INSERT INTO fs.commits (repository_id, parent_commit_id, message)
        VALUES ('${repoId}', NULL, 'Initial commit')
      `);

      const commitResult = await db?.query<{ id: string }>(
        "SELECT id FROM fs.commits WHERE message = 'Initial commit'"
      );
      const commitId = commitResult?.rows[0].id;

      // Write a file
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commitId}', '/test.txt', 'Hello World')
      `);

      // Read the file
      const fileResult = await db?.query<{ content: string }>(
        `SELECT fs.read_file('${commitId}', '/test.txt') as content`
      );

      assert.strictEqual(fileResult?.rows[0].content, "Hello World");
    });
  });

  describe("Version Control", () => {
    let repoId: string;
    let commit1Id: string;
    let commit2Id: string;

    beforeEach(async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>(
        "INSERT INTO fs.repositories (name) VALUES ('version-test') RETURNING *"
      );
      repoId = repoResult!.rows[0].id;

      // Create first commit
      await db?.exec(`
        INSERT INTO fs.commits (repository_id, parent_commit_id, message)
        VALUES ('${repoId}', NULL, 'Commit 1')
      `);

      const commit1Result = await db?.query<{ id: string }>(
        "SELECT id FROM fs.commits WHERE message = 'Commit 1'"
      );
      commit1Id = commit1Result?.rows[0].id!;

      // Create second commit
      await db?.exec(`
        INSERT INTO fs.commits (repository_id, parent_commit_id, message)
        VALUES ('${repoId}', '${commit1Id}', 'Commit 2')
      `);

      const commit2Result = await db?.query<{ id: string }>(
        "SELECT id FROM fs.commits WHERE message = 'Commit 2'"
      );
      commit2Id = commit2Result?.rows[0].id!;
    });

    it("should cascade file reads through commit history", async () => {
      // Write file in commit 1
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commit1Id}', '/persistent.txt', 'Version 1')
      `);

      // File should be readable from both fs.commits
      const result1 = await db?.query<{ content: string }>(
        `SELECT fs.read_file('${commit1Id}', '/persistent.txt') as content`
      );

      const result2 = await db?.query<{ content: string }>(
        `SELECT fs.read_file('${commit2Id}', '/persistent.txt') as content`
      );

      assert.strictEqual(result1?.rows[0].content, "Version 1");
      assert.strictEqual(result2?.rows[0].content, "Version 1");
    });

    it("should override files in newer fs.commits", async () => {
      // Write file in commit 1
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commit1Id}', '/changing.txt', 'Version 1')
      `);

      // Override in commit 2
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commit2Id}', '/changing.txt', 'Version 2')
      `);

      // Check versions
      const result1 = await db?.query<{ content: string }>(
        `SELECT fs.read_file('${commit1Id}', '/changing.txt') as content`
      );

      const result2 = await db?.query<{ content: string }>(
        `SELECT fs.read_file('${commit2Id}', '/changing.txt') as content`
      );

      assert.strictEqual(result1?.rows[0].content, "Version 1");
      assert.strictEqual(result2?.rows[0].content, "Version 2");
    });

    it("should list files from commit and ancestors", async () => {
      // Files in commit 1
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES
        ('${commit1Id}', '/file1.txt', 'Content 1'),
        ('${commit1Id}', '/file2.txt', 'Content 2')
      `);

      // File in commit 2
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commit2Id}', '/file3.txt', 'Content 3')
      `);

      const result = await db?.query<{ path: string }>(
        `SELECT path FROM fs.get_commit_snapshot('${commit2Id}') ORDER BY path`
      );

      assert.strictEqual(result?.rows.length, 3);
      assert.strictEqual(result?.rows[0].path, "/file1.txt");
      assert.strictEqual(result?.rows[1].path, "/file2.txt");
      assert.strictEqual(result?.rows[2].path, "/file3.txt");
    });

    it("should get file history", async () => {
      // Version 1 in commit 1
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commit1Id}', '/history.txt', 'Version 1')
      `);

      // Version 2 in commit 2
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commit2Id}', '/history.txt', 'Version 2')
      `);

      const result = await db?.query<{
        commit_id: string;
        content: string;
        is_deleted: boolean;
        is_symlink: boolean;
      }>(
        `SELECT commit_id, content, is_deleted, is_symlink FROM fs.get_file_history('${commit2Id}', '/history.txt') ORDER BY content`
      );

      assert.strictEqual(result?.rows.length, 2);
      assert.strictEqual(result?.rows[0].content, "Version 1");
      assert.strictEqual(result?.rows[1].content, "Version 2");
      assert.strictEqual(result?.rows[0].is_deleted, false);
      assert.strictEqual(result?.rows[0].is_symlink, false);
      assert.strictEqual(result?.rows[1].is_deleted, false);
      assert.strictEqual(result?.rows[1].is_symlink, false);
    });
  });

  describe("Repository and Branch Management", () => {
    it("should create fs.repositories with default fs.branches", async () => {
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>("INSERT INTO fs.repositories (name) VALUES ('test-repo') RETURNING *");
      const repoId = repoResult?.rows[0].id;

      // Verify repository was created
      const repoCheck = await db?.query<{
        name: string;
        default_branch_id: string;
      }>(
        `SELECT name, default_branch_id FROM fs.repositories WHERE id = '${repoId}'`
      );
      assert.strictEqual(repoCheck?.rows[0].name, "test-repo");
      assert.ok(repoCheck?.rows[0].default_branch_id);

      // Verify default branch was created
      const branchCheck = await db?.query<{
        name: string;
        repository_id: string;
        head_commit_id: string | null;
      }>(
        `SELECT name, repository_id, head_commit_id FROM fs.branches WHERE repository_id = '${repoId}'`
      );
      assert.strictEqual(branchCheck?.rows.length, 1);
      assert.strictEqual(branchCheck?.rows[0].name, "main");
      assert.strictEqual(branchCheck?.rows[0].repository_id, repoId);
      assert.strictEqual(branchCheck?.rows[0].head_commit_id, null); // No initial commit by default

      // Verify no initial commit was created
      const commitCheck = await db?.query<{
        repository_id: string;
        message: string;
        parent_commit_id: string | null;
      }>(
        `SELECT repository_id, message, parent_commit_id FROM fs.commits WHERE repository_id = '${repoId}'`
      );
      assert.strictEqual(commitCheck?.rows.length, 0);
    });

    it("should create additional fs.branches", async () => {
      // Create repository first
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>(
        "INSERT INTO fs.repositories (name) VALUES ('branch-test') RETURNING *"
      );
      const repoId = repoResult!.rows[0].id;

      // Get initial commit and branch count
      const initialBranchCount = await db?.query<{ count: number }>(
        `SELECT COUNT(*) as count FROM fs.branches WHERE repository_id = '${repoId}'`
      );
      const initialCommitCount = await db?.query<{ count: number }>(
        `SELECT COUNT(*) as count FROM fs.commits WHERE repository_id = '${repoId}'`
      );

      // Create additional branch
      const branchResult = await db?.query<{
        id: string;
        name: string;
        head_commit_id: string;
        created_at: string;
      }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature-branch') RETURNING *",
        [repoId]
      );
      const branchId = branchResult!.rows[0].id;

      // Verify branch was created
      const branchCheck = await db?.query<{
        name: string;
        repository_id: string;
        head_commit_id: string | null;
      }>(
        `SELECT name, repository_id, head_commit_id FROM fs.branches WHERE id = '${branchId}'`
      );
      assert.strictEqual(branchCheck?.rows[0].name, "feature-branch");
      assert.strictEqual(branchCheck?.rows[0].repository_id, repoId);
      // By default, new branches start from the repository default branch head
      const mainHead = await db?.query<{ head_commit_id: string | null }>(
        `SELECT head_commit_id FROM fs.branches WHERE repository_id = '${repoId}' AND name = 'main'`
      );
      assert.strictEqual(
        branchCheck?.rows[0].head_commit_id,
        mainHead?.rows[0].head_commit_id
      );

      // Verify repository now has 2 fs.branches
      const allBranches = await db?.query<{ count: number }>(
        `SELECT COUNT(*) as count FROM fs.branches WHERE repository_id = '${repoId}'`
      );
      assert.strictEqual(
        allBranches?.rows[0].count,
        initialBranchCount!.rows[0].count + 1
      );

      // Creating a branch should not create a new commit (branches point at existing commits)
      const allCommits = await db?.query<{ count: number }>(
        `SELECT COUNT(*) as count FROM fs.commits WHERE repository_id = '${repoId}'`
      );
      assert.strictEqual(
        allCommits?.rows[0].count,
        initialCommitCount!.rows[0].count
      );
    });
  });

  describe("Path Normalization", () => {
    let repoId: string;

    let commitId: string;

    beforeEach(async () => {
      // Create repository for each test
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>("INSERT INTO fs.repositories (name) VALUES ('path-test') RETURNING *");
      repoId = repoResult!.rows[0].id;

      // Create a commit for file operations
      const commitResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'Test commit') RETURNING id",
        [repoId]
      );
      commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [commitId, repoId]
      );
    });

    it("should normalize absolute paths", async () => {
      const insertResult = await db?.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, '/src/main.ts', 'content') RETURNING *",
        [commitId]
      );

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should normalize relative paths to absolute", async () => {
      const insertResult = await db?.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, 'src/main.ts', 'content') RETURNING *",
        [commitId]
      );

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should remove duplicate slashes", async () => {
      const insertResult = await db?.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, '//src//main.ts', 'content') RETURNING *",
        [commitId]
      );

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should remove trailing slashes", async () => {
      const insertResult = await db?.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, '/src/main.ts/', 'content') RETURNING *",
        [commitId]
      );

      assert.strictEqual(insertResult?.rows[0].path, "/src/main.ts");
    });

    it("should handle root path", async () => {
      const insertResult = await db?.query<{
        id: string;
        commit_id: string;
        path: string;
        content: string;
        created_at: string;
      }>(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, '/', 'content') RETURNING *",
        [commitId]
      );

      assert.strictEqual(insertResult?.rows[0].path, "/");
    });
  });

  describe("Path Validation", () => {
    it("should reject null paths", async () => {
      try {
        await db?.query("SELECT fs._validate_path(NULL) as _validate_path");
        assert.fail("Expected validation to fail for null path");
      } catch (err: any) {
        assert.match(err.message, /Path cannot be null or empty/);
      }
    });

    it("should reject empty paths", async () => {
      try {
        await db?.query("SELECT fs._validate_path('') as _validate_path");
        assert.fail("Expected validation to fail for empty path");
      } catch (err: any) {
        assert.match(err.message, /Path cannot be null or empty/);
      }
    });

    it("should reject paths with control characters", async () => {
      try {
        await db?.query(
          "SELECT fs._validate_path('/test\x01file.txt') as _validate_path"
        );
        assert.fail(
          "Expected validation to fail for path with control characters"
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }
    });

    it("should reject paths with Windows-invalid characters", async () => {
      // Test characters invalid on Windows: < > : " | ? *
      const invalidChars = ["<", ">", ":", '"', "|", "?", "*"];

      for (const char of invalidChars) {
        try {
          await db?.query(
            `SELECT fs._validate_path('/test${char}file.txt') as _validate_path`
          );
          assert.fail(`Expected validation to fail for path with ${char}`);
        } catch (err: any) {
          assert.match(
            err.message,
            /Path contains characters invalid on Windows/
          );
        }
      }
    });

    it("should reject paths with control characters", async () => {
      // Test control characters (0x00-0x1F except tab, newline, carriage return)
      try {
        await db?.query(
          "SELECT fs._validate_path('/test' || chr(1) || 'file.txt') as _validate_path"
        );
        assert.fail(
          "Expected validation to fail for path with control character"
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }

      try {
        await db?.query(
          "SELECT fs._validate_path('/test' || chr(2) || 'file.txt') as _validate_path"
        );
        assert.fail(
          "Expected validation to fail for path with control character"
        );
      } catch (err: any) {
        assert.match(err.message, /Path contains control characters/);
      }
    });

    it("should reject paths with null bytes", async () => {
      try {
        await db?.query(
          "SELECT fs._validate_path('/test' || chr(0) || 'file.txt') as _validate_path"
        );
        assert.fail("Expected validation to fail for path with null byte");
      } catch (err: any) {
        // PostgreSQL itself rejects null characters, so we accept either our validation error or PostgreSQL's
        assert.ok(
          err.message.includes("Path contains null bytes") ||
            err.message.includes("null character not permitted"),
          `Unexpected error: ${err.message}`
        );
      }
    });

    it("should reject very long paths", async () => {
      const longPath = "/" + "a".repeat(4100);
      try {
        await db?.query("SELECT fs._validate_path($1) as _validate_path", [
          longPath,
        ]);
        assert.fail("Expected validation to fail for very long path");
      } catch (err: any) {
        assert.match(err.message, /Path is too long/);
      }
    });

    it("should accept valid paths", async () => {
      const result = await db?.query<{ _validate_path: any }>(
        "SELECT fs._validate_path('/valid/path/file.txt') as _validate_path"
      );
      // Should not throw an error
      assert.ok(result);
    });
  });

  describe("Real World Usage Scenario", () => {
    it("should demonstrate basic repository and file operations", async () => {
      // Create a repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>("INSERT INTO fs.repositories (name) VALUES ('demo-repo') RETURNING *");
      const repoId = repoResult!.rows[0].id;

      // Create a commit for adding files
      const commitResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'Add initial files') RETURNING id",
        [repoId]
      );
      const commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [commitId, repoId]
      );

      // Add some files to the commit
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, 'index.html', $2)",
        [commitId, "<h1>Hello World</h1>"]
      );

      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, 'styles.css', $2)",
        [commitId, "body { background: #f0f0f0; }"]
      );

      // Verify files in initial commit
      const files = await db?.query<{ path: string }>(
        `SELECT path FROM fs.get_commit_snapshot('${commitId}')`
      );
      assert.strictEqual(files?.rows.length, 2);

      const filePaths = files?.rows.map((f) => f.path).sort();
      assert.deepStrictEqual(filePaths, ["/index.html", "/styles.css"]);

      // Verify exact file contents
      const htmlFile = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [commitId, "/index.html"]
      );
      assert.strictEqual(htmlFile?.rows[0].content, "<h1>Hello World</h1>");

      const cssFile = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [commitId, "/styles.css"]
      );
      assert.strictEqual(
        cssFile?.rows[0].content,
        "body { background: #f0f0f0; }"
      );

      // Create another commit for file updates
      const updateCommitResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Update HTML file', $2) RETURNING id",
        [repoId, commitId]
      );
      const updateCommitId = updateCommitResult!.rows[0].id;

      // Manually update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [updateCommitId, repoId]
      );

      // Modify a file in the new commit
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, '/index.html', $2)",
        [updateCommitId, "<h1>Hello Updated World</h1>"]
      );

      // Verify the updated files
      const updatedFiles = await db?.query<{ path: string }>(
        `SELECT path FROM fs.get_commit_snapshot('${updateCommitId}')`
      );
      assert.strictEqual(updatedFiles?.rows.length, 2);

      const updatedFilePaths = updatedFiles?.rows.map((f) => f.path).sort();
      assert.deepStrictEqual(updatedFilePaths, ["/index.html", "/styles.css"]);

      // Verify exact updated contents
      const updatedHtmlFile = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [updateCommitId, "/index.html"]
      );
      assert.strictEqual(
        updatedHtmlFile?.rows[0].content,
        "<h1>Hello Updated World</h1>"
      );

      const updatedCssFile = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [updateCommitId, "/styles.css"]
      );
      assert.strictEqual(
        updatedCssFile?.rows[0].content,
        "body { background: #f0f0f0; }"
      ); // Should remain unchanged

      // Verify we have multiple fs.commits
      const commitCount = await db?.query<{ count: number }>(
        `SELECT COUNT(*) as count FROM fs.commits WHERE repository_id = '${repoId}'`
      );
      assert.ok(
        commitCount && commitCount.rows[0] && commitCount.rows[0].count >= 2
      ); // 2 manual fs.commits
    });
  });

  describe("Edge Cases", () => {
    it("should return null for non-existent files", async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>("INSERT INTO fs.repositories (name) VALUES ('edge-test') RETURNING *");
      const repoId = repoResult?.rows[0].id;

      // Create a commit manually so we have one to read from
      await db?.exec(`
        INSERT INTO fs.commits (repository_id, parent_commit_id, message)
        VALUES ('${repoId}', NULL, 'Empty commit')
      `);

      const commitResult = await db?.query<{ id: string }>(
        "SELECT id FROM fs.commits WHERE message = 'Empty commit'"
      );
      const commitId = commitResult?.rows[0].id;

      const result = await db?.query<{ content: string | null }>(
        `SELECT fs.read_file('${commitId}', '/nonexistent.txt') as content`
      );

      assert.strictEqual(result?.rows[0].content, null);
    });

    it("should handle empty file content", async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>(
        "INSERT INTO fs.repositories (name) VALUES ('empty-test') RETURNING *"
      );
      const repoId = repoResult?.rows[0].id;

      // Create a commit
      await db?.exec(`
        INSERT INTO fs.commits (repository_id, parent_commit_id, message)
        VALUES ('${repoId}', NULL, 'Empty file commit')
      `);

      const commitResult = await db?.query<{ id: string }>(
        "SELECT id FROM fs.commits WHERE message = 'Empty file commit'"
      );
      const commitId = commitResult?.rows[0].id;

      // Write empty file
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES ('${commitId}', '/empty.txt', '')
      `);

      const result = await db?.query<{ content: string }>(
        `SELECT fs.read_file('${commitId}', '/empty.txt') as content`
      );

      assert.strictEqual(result?.rows[0].content, "");
    });
  });

  describe("Deletions", () => {
    it("should support tombstone deletions via files.is_deleted", async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>(
        "INSERT INTO fs.repositories (name) VALUES ('delete-test') RETURNING *"
      );
      const repoId = repoResult!.rows[0].id;

      // Create commit 1
      const commit1Result = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'Commit 1') RETURNING id",
        [repoId]
      );
      const commit1Id = commit1Result!.rows[0].id;

      // Update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [commit1Id, repoId]
      );

      // Write a file in commit 1
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [commit1Id, "/delete-me.txt", "hello"]
      );

      // Create commit 2
      const commit2Result = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Commit 2', $2) RETURNING id",
        [repoId, commit1Id]
      );
      const commit2Id = commit2Result!.rows[0].id;

      // Update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [commit2Id, repoId]
      );

      // Tombstone delete in commit 2 (no content required)
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, is_deleted) VALUES ($1, $2, TRUE)",
        [commit2Id, "/delete-me.txt"]
      );

      // read_file should normalize and respect tombstones
      const before = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [commit1Id, "delete-me.txt"]
      );
      assert.strictEqual(before?.rows[0].content, "hello");

      const after = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [commit2Id, "delete-me.txt"]
      );
      assert.strictEqual(after?.rows[0].content, null);

      const files = await db?.query<{ path: string }>(
        "SELECT path FROM fs.get_commit_snapshot($1) ORDER BY path",
        [commit2Id]
      );
      assert.deepStrictEqual(
        files?.rows.map((r) => r.path),
        []
      );

      const history = await db?.query<{
        commit_id: string;
        content: string | null;
        is_deleted: boolean;
        is_symlink: boolean;
      }>("SELECT * FROM fs.get_file_history($1, $2)", [
        commit2Id,
        "/delete-me.txt",
      ]);
      assert.strictEqual(history?.rows.length, 2);
      assert.ok(history?.rows.some((r) => r.is_deleted && r.content === null));
      assert.ok(
        history?.rows.some((r) => !r.is_deleted && r.content === "hello")
      );
      assert.ok(history?.rows.every((r) => r.is_symlink === false));
    });
  });

  describe("Symlinks", () => {
    it("should store symlink targets as normalized absolute paths", async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        default_branch_id: string;
        created_at: string;
      }>(
        "INSERT INTO fs.repositories (name) VALUES ('symlink-test') RETURNING *"
      );
      const repoId = repoResult!.rows[0].id;

      // Create commit
      const commitResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'Add symlink') RETURNING id",
        [repoId]
      );
      const commitId = commitResult!.rows[0].id;

      // Update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [commitId, repoId]
      );

      // Target file
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [commitId, "/target.txt", "hello"]
      );

      // Symlink file (target path is stored in content)
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content, is_symlink) VALUES ($1, $2, $3, TRUE)",
        [commitId, "/link.txt", "target.txt"]
      );

      const stored = await db?.query<{
        path: string;
        content: string;
        is_symlink: boolean;
      }>(
        "SELECT path, content, is_symlink FROM fs.files WHERE commit_id = $1 AND path = $2",
        [commitId, "/link.txt"]
      );

      assert.strictEqual(stored?.rows.length, 1);
      assert.strictEqual(stored?.rows[0].is_symlink, true);
      assert.strictEqual(stored?.rows[0].content, "/target.txt"); // normalized to absolute

      const snapshot = await db?.query<{
        path: string;
        is_symlink: boolean;
      }>(
        "SELECT path, is_symlink FROM fs.get_commit_snapshot($1) ORDER BY path",
        [commitId]
      );
      assert.deepStrictEqual(
        snapshot?.rows.map((r) => r.path),
        ["/link.txt", "/target.txt"]
      );
      const link = snapshot?.rows.find((r) => r.path === "/link.txt");
      assert.strictEqual(link?.is_symlink, true);

      // read_file returns the stored content (the link target path) for now
      const read = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [commitId, "/link.txt"]
      );
      assert.strictEqual(read?.rows[0].content, "/target.txt");

      const history = await db?.query<{
        commit_id: string;
        content: string | null;
        is_deleted: boolean;
        is_symlink: boolean;
      }>("SELECT * FROM fs.get_file_history($1, $2)", [commitId, "/link.txt"]);
      assert.strictEqual(history?.rows.length, 1);
      assert.strictEqual(history?.rows[0].is_deleted, false);
      assert.strictEqual(history?.rows[0].is_symlink, true);
      assert.strictEqual(history?.rows[0].content, "/target.txt");
    });
  });

  describe("Merge / Rebase Helpers", () => {
    it("should compute merge base for ancestor relationships", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-base-ancestor') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const rootHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const commitAResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'A', $2) RETURNING id",
        [repoId, rootCommitId]
      );
      const commitAId = commitAResult!.rows[0].id;

      const commitBResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'B', $2) RETURNING id",
        [repoId, commitAId]
      );
      const commitBId = commitBResult!.rows[0].id;

      const base1 = await db?.query<{ base: string }>(
        "SELECT fs.get_merge_base($1, $2) as base",
        [commitAId, commitBId]
      );
      assert.strictEqual(base1?.rows[0].base, commitAId);

      const base2 = await db?.query<{ base: string }>(
        "SELECT fs.get_merge_base($1, $2) as base",
        [commitBId, commitAId]
      );
      assert.strictEqual(base2?.rows[0].base, commitAId);

      const base3 = await db?.query<{ base: string }>(
        "SELECT fs.get_merge_base($1, $2) as base",
        [commitBId, commitBId]
      );
      assert.strictEqual(base3?.rows[0].base, commitBId);
    });

    it("should compute merge base for diverged branches", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-base-diverged') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const rootHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Base', $2) RETURNING id",
        [repoId, rootCommitId]
      );
      const baseCommitId = baseResult!.rows[0].id;

      // Move main forward so the new branch defaults to this base
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [baseCommitId, repoId]
      );

      const featureBranchResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id",
        [repoId]
      );
      const featureBranchId = featureBranchResult!.rows[0].id;

      const main1Result = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const main1Id = main1Result!.rows[0].id;

      const feat1Result = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'feature-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const feat1Id = feat1Result!.rows[0].id;

      // (Not required for merge base, but keep branch heads realistic)
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE repository_id = $2 AND name = 'main'",
        [main1Id, repoId]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [feat1Id, featureBranchId]
      );

      const base = await db?.query<{ base: string }>(
        "SELECT fs.get_merge_base($1, $2) as base",
        [main1Id, feat1Id]
      );
      assert.strictEqual(base?.rows[0].base, baseCommitId);
      assert.notStrictEqual(base?.rows[0].base, rootCommitId);
    });

    it("should reject merge base across repositories", async () => {
      const repo1 = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-base-repo-1') RETURNING id"
      );
      const repo2 = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-base-repo-2') RETURNING id"
      );

      const repo1Id = repo1!.rows[0].id;
      const repo2Id = repo2!.rows[0].id;

      // Create one commit in each repo so we have valid commit ids
      const c1 = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'repo-1-root') RETURNING id",
        [repo1Id]
      );
      const c2 = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'repo-2-root') RETURNING id",
        [repo2Id]
      );

      try {
        await db?.query("SELECT fs.get_merge_base($1, $2)", [
          c1!.rows[0].id,
          c2!.rows[0].id,
        ]);
        assert.fail("Expected merge base to fail across repositories");
      } catch (err: any) {
        assert.match(err.message, /Commits must belong to the same repository/);
      }
    });

    it("should return no conflicts when changes do not overlap", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('conflicts-non-overlap') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const rootHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Base', $2) RETURNING id",
        [repoId, rootCommitId]
      );
      const baseCommitId = baseResult!.rows[0].id;

      const leftResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Left', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const leftCommitId = leftResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [leftCommitId, "/main-only.txt", "main"]
      );

      const rightResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Right', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const rightCommitId = rightResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [rightCommitId, "/feature-only.txt", "feature"]
      );

      const conflicts = await db?.query<{ path: string }>(
        "SELECT path FROM fs.get_conflicts($1, $2)",
        [leftCommitId, rightCommitId]
      );
      assert.strictEqual(conflicts?.rows.length, 0);
    });

    it("should detect modify/modify conflicts", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('conflicts-modify-modify') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const rootHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Base', $2) RETURNING id",
        [repoId, rootCommitId]
      );
      const baseCommitId = baseResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [baseCommitId, "/same.txt", "base"]
      );

      const leftResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Left', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const leftCommitId = leftResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [leftCommitId, "/same.txt", "left"]
      );

      const rightResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Right', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const rightCommitId = rightResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [rightCommitId, "/same.txt", "right"]
      );

      const conflicts = await db?.query<{
        merge_base_commit_id: string;
        path: string;
        base_exists: boolean;
        left_exists: boolean;
        right_exists: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>("SELECT * FROM fs.get_conflicts($1, $2)", [
        leftCommitId,
        rightCommitId,
      ]);

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.merge_base_commit_id, baseCommitId);
      assert.strictEqual(c.path, "/same.txt");
      assert.strictEqual(c.base_exists, true);
      assert.strictEqual(c.left_exists, true);
      assert.strictEqual(c.right_exists, true);
      assert.strictEqual(c.base_content, "base");
      assert.strictEqual(c.left_content, "left");
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "modify/modify");
    });

    it("should detect delete/modify conflicts", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('conflicts-delete-modify') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const rootHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Base', $2) RETURNING id",
        [repoId, rootCommitId]
      );
      const baseCommitId = baseResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [baseCommitId, "/del.txt", "base"]
      );

      const leftResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Left', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const leftCommitId = leftResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, is_deleted) VALUES ($1, $2, TRUE)",
        [leftCommitId, "/del.txt"]
      );

      const rightResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Right', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const rightCommitId = rightResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [rightCommitId, "/del.txt", "right"]
      );

      const conflicts = await db?.query<{
        path: string;
        base_exists: boolean;
        left_exists: boolean;
        right_exists: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>(
        "SELECT path, base_exists, left_exists, right_exists, base_content, left_content, right_content, conflict_kind FROM fs.get_conflicts($1, $2)",
        [leftCommitId, rightCommitId]
      );

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.path, "/del.txt");
      assert.strictEqual(c.base_exists, true);
      assert.strictEqual(c.left_exists, false);
      assert.strictEqual(c.right_exists, true);
      assert.strictEqual(c.base_content, "base");
      assert.strictEqual(c.left_content, null);
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "delete/modify");
    });

    it("should detect add/add conflicts", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('conflicts-add-add') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const rootHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Base', $2) RETURNING id",
        [repoId, rootCommitId]
      );
      const baseCommitId = baseResult!.rows[0].id;

      const leftResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Left', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const leftCommitId = leftResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [leftCommitId, "/new.txt", "left"]
      );

      const rightResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Right', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const rightCommitId = rightResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [rightCommitId, "/new.txt", "right"]
      );

      const conflicts = await db?.query<{
        path: string;
        base_exists: boolean;
        left_exists: boolean;
        right_exists: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>(
        "SELECT path, base_exists, left_exists, right_exists, base_content, left_content, right_content, conflict_kind FROM fs.get_conflicts($1, $2)",
        [leftCommitId, rightCommitId]
      );

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.path, "/new.txt");
      assert.strictEqual(c.base_exists, false);
      assert.strictEqual(c.left_exists, true);
      assert.strictEqual(c.right_exists, true);
      assert.strictEqual(c.base_content, null);
      assert.strictEqual(c.left_content, "left");
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "add/add");
    });

    it("should treat symlink/file differences as conflicts", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('conflicts-symlink-file') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const rootHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const rootCommitId = rootHead!.rows[0].head_commit_id;

      const baseResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Base', $2) RETURNING id",
        [repoId, rootCommitId]
      );
      const baseCommitId = baseResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [baseCommitId, "/thing.txt", "base"]
      );

      const leftResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Left', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const leftCommitId = leftResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content, is_symlink) VALUES ($1, $2, $3, TRUE)",
        [leftCommitId, "/thing.txt", "target.txt"]
      );

      const rightResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'Right', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const rightCommitId = rightResult!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [rightCommitId, "/thing.txt", "right"]
      );

      const conflicts = await db?.query<{
        path: string;
        base_is_symlink: boolean;
        left_is_symlink: boolean;
        right_is_symlink: boolean;
        base_content: string | null;
        left_content: string | null;
        right_content: string | null;
        conflict_kind: string;
      }>(
        "SELECT path, base_is_symlink, left_is_symlink, right_is_symlink, base_content, left_content, right_content, conflict_kind FROM fs.get_conflicts($1, $2)",
        [leftCommitId, rightCommitId]
      );

      assert.strictEqual(conflicts?.rows.length, 1);
      const c = conflicts!.rows[0];
      assert.strictEqual(c.path, "/thing.txt");
      assert.strictEqual(c.base_is_symlink, false);
      assert.strictEqual(c.left_is_symlink, true);
      assert.strictEqual(c.right_is_symlink, false);
      assert.strictEqual(c.base_content, "base");
      assert.strictEqual(c.left_content, "/target.txt"); // normalized absolute target
      assert.strictEqual(c.right_content, "right");
      assert.strictEqual(c.conflict_kind, "modify/modify");
    });

    it("should reject conflict checks for invalid commit ids", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('conflicts-invalid-ids') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const goodCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'good') RETURNING id",
        [repoId]
      );
      const goodCommitId = goodCommit!.rows[0].id;

      try {
        await db?.query("SELECT * FROM fs.get_conflicts($1, $2)", [
          goodCommitId,
          "00000000-0000-0000-0000-000000000000",
        ]);
        assert.fail("Expected conflict check to fail for invalid commit id");
      } catch (err: any) {
        assert.match(
          err.message,
          /Invalid commit_id \(right\): commit does not exist/
        );
      }
    });
  });

  describe("Merge / Rebase Operations", () => {
    it("should finalize merge by applying non-conflicting changes", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-non-conflict') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string | null;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );

      const featureBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id, head_commit_id",
        [repoId]
      );
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const mainCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const mainCommitId = mainCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mainCommitId, "/main.txt", "main"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [mainCommitId, mainBranchId]
      );

      const featCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'feature-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const featCommitId = featCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [featCommitId, "/feature.txt", "feature"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [featCommitId, featureBranchId]
      );

      const mergeCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES ($1, 'Merge feature into main', $2, $3) RETURNING id",
        [repoId, mainCommitId, featCommitId]
      );
      const mergeCommitId = mergeCommit!.rows[0].id;

      const mergeResult = await db?.query<{
        operation: string;
        merge_commit_id: string | null;
        new_target_head_commit_id: string;
        applied_file_count: number;
      }>(
        "SELECT operation, merge_commit_id, new_target_head_commit_id, applied_file_count FROM fs.finalize_commit($1, $2)",
        [mergeCommitId, mainBranchId]
      );

      assert.strictEqual(mergeResult?.rows.length, 1);
      assert.strictEqual(mergeResult?.rows[0].operation, "merged");
      assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
      assert.strictEqual(
        mergeResult?.rows[0].new_target_head_commit_id,
        mergeCommitId
      );
      assert.strictEqual(mergeResult?.rows[0].applied_file_count, 1);

      const snapshot = await db?.query<{ path: string }>(
        "SELECT path FROM fs.get_commit_snapshot($1) ORDER BY path",
        [mergeCommitId]
      );
      assert.deepStrictEqual(
        snapshot?.rows.map((r) => r.path),
        ["/feature.txt", "/main.txt"]
      );
    });

    it("should require conflict resolutions before finalizing", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-conflict-required') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [baseCommitId, "/same.txt", "base"]
      );

      const featureBranch = await db?.query<{ id: string }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id",
        [repoId]
      );
      const featureBranchId = featureBranch!.rows[0].id;

      const mainCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const mainCommitId = mainCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mainCommitId, "/same.txt", "main"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [mainCommitId, mainBranchId]
      );

      const featCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'feature-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const featCommitId = featCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [featCommitId, "/same.txt", "feature"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [featCommitId, featureBranchId]
      );

      const mergeCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES ($1, 'Merge with conflict', $2, $3) RETURNING id",
        [repoId, mainCommitId, featCommitId]
      );
      const mergeCommitId = mergeCommit!.rows[0].id;

      try {
        await db?.query("SELECT * FROM fs.finalize_commit($1, $2)", [
          mergeCommitId,
          mainBranchId,
        ]);
        assert.fail("Expected merge to fail without conflict resolutions");
      } catch (err: any) {
        assert.match(err.message, /Merge requires resolutions/);
      }

      const mainHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE id = $1",
        [mainBranchId]
      );
      assert.strictEqual(mainHead?.rows[0].head_commit_id, mainCommitId);
    });

    it("should honor user-provided conflict resolutions", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-conflict-resolution') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [baseCommitId, "/same.txt", "base"]
      );

      const featureBranch = await db?.query<{ id: string }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id",
        [repoId]
      );
      const featureBranchId = featureBranch!.rows[0].id;

      const mainCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const mainCommitId = mainCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mainCommitId, "/same.txt", "main"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [mainCommitId, mainBranchId]
      );

      const featCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'feature-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const featCommitId = featCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [featCommitId, "/same.txt", "feature"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [featCommitId, featureBranchId]
      );

      const mergeCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES ($1, 'Merge with resolution', $2, $3) RETURNING id",
        [repoId, mainCommitId, featCommitId]
      );
      const mergeCommitId = mergeCommit!.rows[0].id;

      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mergeCommitId, "/same.txt", "resolved"]
      );

      const mergeResult = await db?.query<{
        operation: string;
        merge_commit_id: string | null;
        new_target_head_commit_id: string;
      }>(
        "SELECT operation, merge_commit_id, new_target_head_commit_id FROM fs.finalize_commit($1, $2)",
        [mergeCommitId, mainBranchId]
      );

      assert.strictEqual(mergeResult?.rows.length, 1);
      assert.strictEqual(
        mergeResult?.rows[0].operation,
        "merged_with_conflicts_resolved"
      );
      assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
      assert.strictEqual(
        mergeResult?.rows[0].new_target_head_commit_id,
        mergeCommitId
      );

      const resolved = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [mergeCommitId, "/same.txt"]
      );
      assert.strictEqual(resolved?.rows[0].content, "resolved");
    });

    it("should report already_up_to_date when source is ancestor of target", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('merge-already-up-to-date') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [baseCommitId, "/same.txt", "base"]
      );

      const mainCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const mainCommitId = mainCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mainCommitId, "/same.txt", "same"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [mainCommitId, mainBranchId]
      );

      const mergeCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id, merged_from_commit_id) VALUES ($1, 'Merge noop', $2, $3) RETURNING id",
        [repoId, mainCommitId, baseCommitId]
      );
      const mergeCommitId = mergeCommit!.rows[0].id;

      const mergeResult = await db?.query<{
        operation: string;
        merge_commit_id: string | null;
        new_target_head_commit_id: string;
        applied_file_count: number;
      }>(
        "SELECT operation, merge_commit_id, new_target_head_commit_id, applied_file_count FROM fs.finalize_commit($1, $2)",
        [mergeCommitId, mainBranchId]
      );

      assert.strictEqual(mergeResult?.rows.length, 1);
      assert.strictEqual(mergeResult?.rows[0].operation, "already_up_to_date");
      assert.strictEqual(mergeResult?.rows[0].merge_commit_id, mergeCommitId);
      assert.strictEqual(
        mergeResult?.rows[0].new_target_head_commit_id,
        mergeCommitId
      );
      assert.strictEqual(mergeResult?.rows[0].applied_file_count, 0);
    });

    it("should fast-forward rebase when branch is behind onto", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('rebase-ff') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );

      const featureBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id, head_commit_id",
        [repoId]
      );
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const mainCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const mainCommitId = mainCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mainCommitId, "/main.txt", "main"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [mainCommitId, mainBranchId]
      );

      const rebaseResult = await db?.query<{
        operation: string;
        rebased_commit_id: string | null;
        new_branch_head_commit_id: string;
        applied_file_count: number;
      }>(
        "SELECT operation, rebased_commit_id, new_branch_head_commit_id, applied_file_count FROM fs.rebase_branch($1, $2, $3)",
        [featureBranchId, mainBranchId, "Rebase feature onto main"]
      );
      assert.strictEqual(rebaseResult?.rows[0].operation, "fast_forward");
      assert.strictEqual(rebaseResult?.rows[0].rebased_commit_id, null);
      assert.strictEqual(
        rebaseResult?.rows[0].new_branch_head_commit_id,
        mainCommitId
      );
      assert.strictEqual(rebaseResult?.rows[0].applied_file_count, 0);

      const featureHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE id = $1",
        [featureBranchId]
      );
      assert.strictEqual(featureHead?.rows[0].head_commit_id, mainCommitId);
    });

    it("should rebase diverged branch by creating a new linear commit (no conflicts)", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('rebase-diverged') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );

      const featureBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id, head_commit_id",
        [repoId]
      );
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const featCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'feature-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const featCommitId = featCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [featCommitId, "/feature.txt", "feature"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [featCommitId, featureBranchId]
      );

      const mainCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const mainCommitId = mainCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mainCommitId, "/main.txt", "main"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [mainCommitId, mainBranchId]
      );

      const rebaseResult = await db?.query<{
        operation: string;
        rebased_commit_id: string | null;
        new_branch_head_commit_id: string;
        applied_file_count: number;
      }>(
        "SELECT operation, rebased_commit_id, new_branch_head_commit_id, applied_file_count FROM fs.rebase_branch($1, $2, $3)",
        [featureBranchId, mainBranchId, "Rebase feature onto main"]
      );

      assert.strictEqual(rebaseResult?.rows.length, 1);
      assert.strictEqual(rebaseResult?.rows[0].operation, "rebased");
      assert.ok(rebaseResult?.rows[0].rebased_commit_id);
      assert.strictEqual(
        rebaseResult?.rows[0].rebased_commit_id,
        rebaseResult?.rows[0].new_branch_head_commit_id
      );
      assert.strictEqual(rebaseResult?.rows[0].applied_file_count, 1);

      const rebasedCommitId = rebaseResult!.rows[0].rebased_commit_id!;

      const parent = await db?.query<{ parent_commit_id: string }>(
        "SELECT parent_commit_id FROM fs.commits WHERE id = $1",
        [rebasedCommitId]
      );
      assert.strictEqual(parent?.rows[0].parent_commit_id, mainCommitId);

      const snapshot = await db?.query<{ path: string }>(
        "SELECT path FROM fs.get_commit_snapshot($1) ORDER BY path",
        [rebasedCommitId]
      );
      assert.deepStrictEqual(
        snapshot?.rows.map((r) => r.path),
        ["/feature.txt", "/main.txt"]
      );

      const feature = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [rebasedCommitId, "/feature.txt"]
      );
      assert.strictEqual(feature?.rows[0].content, "feature");

      const main = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [rebasedCommitId, "/main.txt"]
      );
      assert.strictEqual(main?.rows[0].content, "main");
    });

    it("should fail rebase on conflict and leave branch head unchanged", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('rebase-conflict') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [baseCommitId, "/same.txt", "base"]
      );

      const featureBranch = await db?.query<{ id: string }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id",
        [repoId]
      );
      const featureBranchId = featureBranch!.rows[0].id;

      const featCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'feature-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const featCommitId = featCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [featCommitId, "/same.txt", "feature"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [featCommitId, featureBranchId]
      );

      const mainCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'main-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const mainCommitId = mainCommit!.rows[0].id;
      await db?.query(
        "INSERT INTO fs.files (commit_id, path, content) VALUES ($1, $2, $3)",
        [mainCommitId, "/same.txt", "main"]
      );
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [mainCommitId, mainBranchId]
      );

      try {
        await db?.query("SELECT * FROM fs.rebase_branch($1, $2, $3)", [
          featureBranchId,
          mainBranchId,
          "Rebase with conflict",
        ]);
        assert.fail("Expected rebase to fail on conflict");
      } catch (err: any) {
        assert.match(err.message, /Rebase blocked by/);
      }

      const featureHead = await db?.query<{ head_commit_id: string }>(
        "SELECT head_commit_id FROM fs.branches WHERE id = $1",
        [featureBranchId]
      );
      assert.strictEqual(featureHead?.rows[0].head_commit_id, featCommitId);
    });

    it("should noop rebase when onto head is already an ancestor of the branch head", async () => {
      const repoResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.repositories (name) VALUES ('rebase-noop') RETURNING id"
      );
      const repoId = repoResult!.rows[0].id;

      const mainBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "SELECT id, head_commit_id FROM fs.branches WHERE repository_id = $1 AND name = 'main'",
        [repoId]
      );
      const mainBranchId = mainBranch!.rows[0].id;
      const rootHeadId = mainBranch!.rows[0].head_commit_id;

      const baseCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'base', $2) RETURNING id",
        [repoId, rootHeadId]
      );
      const baseCommitId = baseCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [baseCommitId, mainBranchId]
      );

      const featureBranch = await db?.query<{
        id: string;
        head_commit_id: string;
      }>(
        "INSERT INTO fs.branches (repository_id, name) VALUES ($1, 'feature') RETURNING id, head_commit_id",
        [repoId]
      );
      const featureBranchId = featureBranch!.rows[0].id;
      assert.strictEqual(featureBranch!.rows[0].head_commit_id, baseCommitId);

      const featCommit = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message, parent_commit_id) VALUES ($1, 'feature-1', $2) RETURNING id",
        [repoId, baseCommitId]
      );
      const featCommitId = featCommit!.rows[0].id;
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [featCommitId, featureBranchId]
      );

      const rebaseResult = await db?.query<{
        operation: string;
        rebased_commit_id: string | null;
        new_branch_head_commit_id: string;
      }>(
        "SELECT operation, rebased_commit_id, new_branch_head_commit_id FROM fs.rebase_branch($1, $2, $3)",
        [featureBranchId, mainBranchId, "Rebase noop"]
      );

      assert.strictEqual(rebaseResult?.rows[0].operation, "already_up_to_date");
      assert.strictEqual(rebaseResult?.rows[0].rebased_commit_id, null);
      assert.strictEqual(
        rebaseResult?.rows[0].new_branch_head_commit_id,
        featCommitId
      );
    });
  });

  describe("Content Browsing", () => {
    let repoId: string;
    let commitId: string;
    let branchId: string;

    beforeEach(async () => {
      // Create repository
      const repoResult = await db?.query<{
        id: string;
        name: string;
        created_at: string;
      }>(
        "INSERT INTO fs.repositories (name) VALUES ('browse-test') RETURNING *"
      );
      repoId = repoResult!.rows[0].id;

      // Get the default branch ID (created by the AFTER INSERT trigger)
      const branchResult = await db?.query<{ default_branch_id: string }>(
        `SELECT default_branch_id FROM fs.repositories WHERE id = '${repoId}'`
      );
      branchId = branchResult!.rows[0].default_branch_id;

      // Create a commit with some files
      const commitResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'Test commit') RETURNING id",
        [repoId]
      );
      commitId = commitResult!.rows[0].id;

      // Manually update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [commitId, branchId]
      );

      // Add some files
      await db?.exec(`
        INSERT INTO fs.files (commit_id, path, content)
        VALUES
          ('${commitId}', '/index.html', '<h1>Hello</h1>'),
          ('${commitId}', '/styles.css', 'body { color: red; }'),
          ('${commitId}', '/script.js', 'console.log(\"hi\");')
      `);
    });

    it("should browse commit delta with fs.get_commit_delta", async () => {
      const contents = await db?.query<{
        repository_id: string;
        repository_name: string;
        commit_id: string;
        path: string;
        is_deleted: boolean;
        is_symlink: boolean;
      }>(
        `SELECT repository_id, repository_name, commit_id, path, is_deleted, is_symlink FROM fs.get_commit_delta('${commitId}') ORDER BY path`
      );

      assert.strictEqual(contents?.rows.length, 3);
      assert.strictEqual(contents?.rows[0].repository_name, "browse-test");
      assert.strictEqual(contents?.rows[0].commit_id, commitId);
      assert.strictEqual(contents?.rows[0].path, "/index.html");
      assert.strictEqual(contents?.rows[0].is_deleted, false);
      assert.strictEqual(contents?.rows[0].is_symlink, false);
      assert.strictEqual(contents?.rows[1].path, "/script.js");
      assert.strictEqual(contents?.rows[2].path, "/styles.css");

      const html = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [commitId, "/index.html"]
      );
      assert.strictEqual(html?.rows[0].content, "<h1>Hello</h1>");
    });

    it("should browse commit snapshot with fs.get_commit_snapshot", async () => {
      const snapshot = await db?.query<{
        repository_id: string;
        repository_name: string;
        commit_id: string;
        path: string;
        is_symlink: boolean;
        commit_created_at: string;
        commit_message: string;
      }>(
        `SELECT repository_id, repository_name, commit_id, path, is_symlink, commit_created_at, commit_message FROM fs.get_commit_snapshot('${commitId}') ORDER BY path`
      );

      assert.strictEqual(snapshot?.rows.length, 3);
      assert.strictEqual(snapshot?.rows[0].repository_name, "browse-test");
      assert.strictEqual(snapshot?.rows[0].commit_id, commitId);
      assert.strictEqual(snapshot?.rows[0].commit_message, "Test commit");
      assert.ok(snapshot?.rows[0].commit_created_at);
      assert.strictEqual(snapshot?.rows[0].path, "/index.html");
      assert.strictEqual(snapshot?.rows[1].path, "/script.js");
      assert.strictEqual(snapshot?.rows[2].path, "/styles.css");

      const html = await db?.query<{ content: string | null }>(
        "SELECT fs.read_file($1, $2) as content",
        [commitId, "/index.html"]
      );
      assert.strictEqual(html?.rows[0].content, "<h1>Hello</h1>");
    });

    it("should browse branch delta using fs.get_commit_delta with branch resolution", async () => {
      const contents = await db?.query<{
        repository_id: string;
        repository_name: string;
        commit_id: string;
        path: string;
        is_deleted: boolean;
        is_symlink: boolean;
        branch_name: string;
      }>(`
        SELECT gcd.repository_id, gcd.repository_name, gcd.commit_id, gcd.path, gcd.is_deleted, gcd.is_symlink, b.name as branch_name
        FROM fs.get_commit_delta((SELECT head_commit_id FROM fs.branches WHERE id = '${branchId}')) gcd
        CROSS JOIN fs.branches b WHERE b.id = '${branchId}'
        ORDER BY gcd.path
      `);

      assert.strictEqual(contents?.rows.length, 3);
      assert.strictEqual(contents?.rows[0].repository_name, "browse-test");
      assert.strictEqual(contents?.rows[0].branch_name, "main");
      assert.strictEqual(contents?.rows[0].commit_id, commitId);
      assert.strictEqual(contents?.rows[0].path, "/index.html");
      assert.strictEqual(contents?.rows[1].path, "/script.js");
      assert.strictEqual(contents?.rows[2].path, "/styles.css");
    });

    it("should return empty result for commit with no files", async () => {
      // Create a commit with no files
      const emptyCommitResult = await db?.query<{ id: string }>(
        "INSERT INTO fs.commits (repository_id, message) VALUES ($1, 'Empty commit') RETURNING id",
        [repoId]
      );
      const emptyCommitId = emptyCommitResult!.rows[0].id;

      // Manually update branch head
      await db?.query(
        "UPDATE fs.branches SET head_commit_id = $1 WHERE id = $2",
        [emptyCommitId, branchId]
      );

      const contents = await db?.query(
        `SELECT * FROM fs.get_commit_delta('${emptyCommitId}')`
      );

      assert.strictEqual(contents?.rows.length, 0);
    });

    it("should include commit metadata", async () => {
      const contents = await db?.query<{
        commit_created_at: string;
        commit_message: string;
      }>(
        `SELECT commit_created_at, commit_message FROM fs.get_commit_delta('${commitId}') LIMIT 1`
      );

      assert.ok(contents?.rows[0].commit_created_at);
      assert.strictEqual(contents?.rows[0].commit_message, "Test commit");
    });
  });
});
