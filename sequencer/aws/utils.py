import boto.utils
import boto.ec2
from sequencer.constants import autoscaling_grp_tag


def get_instance_id():
    return boto.utils.get_instance_metadata()['instance-id']


def get_autoscaling_grp_name(instance_id, aws_region):
    conn = boto.ec2.connect_to_region(aws_region)
    reservation = conn.get_all_reservations(instance_ids=instance_id)
    if autoscaling_grp_tag in reservation[0].instances[0].tags:
        return reservation[0].instances[0].tags[autoscaling_grp_tag]

    raise RuntimeError('unable to find autoscaling group for instance {0}'.format(instance_id))
