import boto.utils


def get_instance_id(**optionals):
    return boto.utils.get_instance_metadata()['instance-id']
