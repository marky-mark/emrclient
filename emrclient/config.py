import json

class Config:
    def __init__(self, master_address, s3_bucket, region, cluster_id):
        self.master_address = master_address
        self.s3_bucket = s3_bucket
        self.region = region
        self.cluster_id = cluster_id

    def to_JSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)