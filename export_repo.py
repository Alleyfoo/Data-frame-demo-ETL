#!/usr/bin/env python3
"""
Repository Export Script
Creates a single text file containing all source files with their paths preserved.

Usage:
    python export_repo.py <repo_path> [output_file]

Example:
    python export_repo.py ./my-repo repo_export.txt
"""

import os
import sys
from pathlib import Path


# File extensions to include (text/source files)
INCLUDE_EXTENSIONS = {
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".json",
    ".py",
    ".java",
    ".c",
    ".cpp",
    ".h",
    ".hpp",
    ".go",
    ".rs",
    ".rb",
    ".php",
    ".html",
    ".htm",
    ".css",
    ".scss",
    ".sass",
    ".less",
    ".md",
    ".txt",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".xml",
    ".sql",
    ".sh",
    ".bash",
    ".zsh",
    ".prisma",
    ".graphql",
    ".gql",
}

# Directories and files to exclude
EXCLUDE_DIRS = {
    "node_modules",
    ".git",
    "__pycache__",
    "venv",
    "env",
    ".venv",
    "dist",
    "build",
    "target",
    "bin",
    "obj",
    ".next",
    ".nuxt",
    "coverage",
    ".pytest_cache",
    "vendor",
    "bower_components",
}

EXCLUDE_FILES = {
    ".DS_Store",
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    ".gitignore",
}

# Max file size to read (in bytes) - skip very large files
MAX_FILE_SIZE = 500 * 1024  # 500 KB


def should_include_file(filepath: Path) -> bool:
    """Check if a file should be included in the export."""
    # Check extension
    if filepath.suffix.lower() not in INCLUDE_EXTENSIONS:
        return False

    # Check filename exclusions
    if filepath.name in EXCLUDE_FILES:
        return False

    # Check file size
    try:
        if filepath.stat().st_size > MAX_FILE_SIZE:
            return False
    except OSError:
        return False

    return True


def read_file_content(filepath: Path) -> str:
    """Read file content with error handling."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception as e:
        return f"[Error reading file: {e}]"


def export_repository(repo_path: str, output_file: str) -> None:
    """Export all files in repository to a single text file."""
    repo_dir = Path(repo_path).resolve()

    if not repo_dir.exists():
        print(f"Error: Directory '{repo_path}' does not exist.")
        sys.exit(1)

    print(f"Exporting repository: {repo_dir}")
    print(f"Output file: {output_file}")
    print("-" * 50)

    file_count = 0
    skipped_count = 0

    with open(output_file, "w", encoding="utf-8") as out:
        out.write("=" * 80 + "\n")
        out.write("REPOSITORY EXPORT\n")
        out.write("=" * 80 + "\n\n")
        out.write(f"Repository Path: {repo_dir}\n\n")
        out.write("-" * 80 + "\n\n")

        # Walk through directory tree
        for root, dirs, files in os.walk(repo_dir):
            # Filter out excluded directories in-place
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

            for filename in files:
                filepath = Path(root) / filename
                rel_path = filepath.relative_to(repo_dir)

                if should_include_file(filepath):
                    # Write file header
                    out.write("=" * 80 + "\n")
                    out.write(f"FILE: {rel_path}\n")
                    out.write("=" * 80 + "\n\n")

                    # Write file content
                    content = read_file_content(filepath)
                    out.write(content)

                    # Add separator between files
                    out.write("\n\n" + "-" * 80 + "\n\n")

                    file_count += 1
                    print(f"✓ Exported: {rel_path}")
                else:
                    skipped_count += 1

        # Write summary
        out.write("\n\n")
        out.write("=" * 80 + "\n")
        out.write("EXPORT SUMMARY\n")
        out.write("=" * 80 + "\n")
        out.write(f"Total Files Exported: {file_count}\n")
        out.write(f"Files Skipped: {skipped_count}\n")
        out.write(f"Repository: {repo_dir}\n")

    print("-" * 50)
    print(f"\n✓ Export complete!")
    print(f"  - {file_count} files exported")
    print(f"  - {skipped_count} files skipped")
    print(f"  - Output: {output_file}")
    print(
        f"\nYou can now copy and paste the contents of '{output_file}' to share the repository."
    )


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Repository Export Script")
        print("-" * 40)
        print("Usage: python export_repo.py <repo_path> [output_file]")
        print("\nArguments:")
        print("  repo_path     - Path to the repository to export")
        print(
            "  output_file   - (Optional) Name of output text file (default: repo_export.txt)"
        )
        print("\nExample:")
        print("  python export_repo.py ./my-project my_repo.txt")
        sys.exit(1)

    repo_path = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "repo_export.txt"

    export_repository(repo_path, output_file)


if __name__ == "__main__":
    main()
