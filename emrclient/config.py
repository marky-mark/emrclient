import json

class Config:
    def __init__(self, master_address, s3_bucket):
        self.master_address = master_address
        self.s3_bucket = s3_bucket

    def to_JSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)