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
    "/b/.vault"
]


class TestMPIWalker(unittest.TestCase):
    def test_basic(self):
        root = MPIStatFile(test_structure[0], 0)
        for path in test_structure[1:]:
            root.insert_child(MPIStatFile(path, 0))
        vaults = set([x.path for x in list(MPIStatFile.find_vaults(root))])
        self.assertEqual(vaults, {MPIStatFile(
            "/a/a/.vault", 0).path, MPIStatFile("/b/.vault", 0).path})


if __name__ == '__main__':
    unittest.main()
