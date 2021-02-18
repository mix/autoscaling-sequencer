import boto
import boto.ec2
import boto.ec2.autoscale
import logging

from .coordinator import DistributedSequenceCoordinator


class Sequencer(object):
    def __init__(self, zookeeper_connect, autoscaling_grp_name, strategy_name, instance_id, max_num_ids, aws_region):
        self.zookeeper_connect = zookeeper_connect
        self.autoscaling_grp_name = autoscaling_grp_name
        self.aws_region = aws_region
        self.strategy_name = strategy_name
        self.instance_id = instance_id
        self.max_num_ids = max_num_ids

    def generate(self):
        try:
            autoscaling_grp = \
                boto.ec2.autoscale.connect_to_region(self.aws_region).get_all_groups([self.autoscaling_grp_name])[0]

            asg_instances_ids = [i.instance_id for i in autoscaling_grp.instances]
            logging.debug('found instances: {0} in autoscaling grp: {1}'.format(
                asg_instances_ids, self.autoscaling_grp_name))
            if self.instance_id not in asg_instances_ids:
                raise RuntimeError('{0} instance does not exists in {1} autoscaling_group'
                                   .format(self.instance_id, self.autoscaling_grp_name))
            max_sequence_id = self.max_num_ids - 1
            sequence_coordinator = DistributedSequenceCoordinator(self.zookeeper_connect,
                                                                  self.autoscaling_grp_name,
                                                                  self.strategy_name,
                                                                  self.instance_id,
                                                                  max_sequence_id,
                                                                  asg_instances_ids)
            return sequence_coordinator.execute()
        except Exception as e:
            raise RuntimeError(e)
