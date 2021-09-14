class DirectoryReport:
    def __init__(self, files, mtime, scratch_disk):
        self.size = 0
        self.bam = 0
        self.cram = 0
        self.vcf = 0
        self.pedbed = 0
        self.num_files = files
        self.mtime = mtime
        self.pi = None
        self.group_name = None
        self.scratch_disk = scratch_disk
