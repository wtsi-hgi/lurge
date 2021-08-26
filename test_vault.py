import unittest

from lurge_types.vault import MPIStatFile

test_structure = [
    "/",
    "/a",
    "/a/a",
    "/a/a/a",
    "/a/a/b",
    "/a/a/b/a",
    "/a/a/b/b",
    "/a/a/b/b/a",
    "/a/a/.vault",
    "/a/a/.vault/vault",
    "/a/b",
    "/a/b/a",
    "/b",
    "/b/a",
    "/b/a/a",
    "/b/a/a/a",
    "/b/a/a/a/a",
    "/b/a/a/a/b",
    "/b/a/a/b",
    "/b/a/a/b/a",
    "/b/a/a/b/b",
    "/b/a/b",
    "/b/b",
    "/b/b/a",
    "/b/b/a/a/",
    "/b/.vault",
    "/b/.vault/vault"
]


def create_file_structure():
    root = MPIStatFile(test_structure[0], 0, 0, 0, 0)
    for path in test_structure[1:]:
        root.insert_child(MPIStatFile(path, 0, 0, 0, 0))
    return root


class TestMPIWalker(unittest.TestCase):

    def test_find_vault(self):
        root = create_file_structure()
        vaults = set([x.path for x in list(MPIStatFile.find_vaults(root))])
        self.assertEqual(vaults, {MPIStatFile(
            "/a/a/.vault", 0, 0, 0, 0).path, MPIStatFile("/b/.vault", 0, 0, 0, 0).path})

    def test_find_files(self):
        root = create_file_structure()
        files = set([x.path for x in list(MPIStatFile.find_files(root))])
        self.assertEqual(files, {
            "a/a/a",
            "a/a/b/a",
            "a/a/b/b/a",
            "a/a/.vault/vault",
            "a/b/a",
            "b/a/a/a/a",
            "b/a/a/a/b",
            "b/a/a/b/a",
            "b/a/a/b/b",
            "b/a/b",
            "b/b/a/a",
            "b/.vault/vault"
        })

    def test_find_file_by_path(self):
        root = create_file_structure()
        expected = [
            "a/a/b/b/a",
            "b/a/a/b/b",
            "b/b/a/a"
        ]
        got = [
            MPIStatFile.find_by_path(root, "/a/a/b/b/a").path,
            MPIStatFile.find_by_path(root, "/b/a/a/b/b").path,
            MPIStatFile.find_by_path(root, "/b/b/a/a").path
        ]
        self.assertEqual(got, expected)


if __name__ == '__main__':
    unittest.main()
