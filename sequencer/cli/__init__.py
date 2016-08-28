import click

from .. import sequencer
from sequencer.aws.utils import get_instance_id, get_autoscaling_grp_name

from sequencer.constants import id_generation_strategies, default_ids_range, default_aws_region

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'], terminal_width=800, max_content_width=800)


@click.command(help='autoscaling-sequencer - Create Unique Ids for instances in a given auto-scaling group',
               context_settings=CONTEXT_SETTINGS)
@click.option('-z', '--zookeeper-connect', metavar='', type=click.STRING, help='Zookeeper Connection String',
              required=True)
@click.option('-s', '--strategy', type=click.Choice(id_generation_strategies),
              default=id_generation_strategies[0], help='ID Generation Strategy', show_default=True)
@click.option('-i', '--instance-id', type=click.STRING, metavar='',
              help='Instance-Id to generate id for. By default takes current instance-id from aws metadata')
@click.option('-a', '--autoscaling-group', type=click.STRING, metavar='',
              help='Autoscaling Group Name. By default gets autoscaling group name of instance')
@click.option('-m', '--max-id-range', type=click.INT, default=default_ids_range, metavar='',
              help='Maximum number of Ids that can be generated', show_default=True)
@click.option('-r', '--aws-region', type=click.STRING, default=default_aws_region, metavar='',
              help='AWS Region', show_default=True)
def generate(zookeeper_connect, autoscaling_group, strategy, instance_id, max_id_range, aws_region):

    if not instance_id:
        instance_id = get_instance_id()

    if not autoscaling_group:
        autoscaling_group = get_autoscaling_grp_name(instance_id, aws_region)

    sequence_generator = sequencer.Sequencer(zookeeper_connect,
                                             autoscaling_group,
                                             strategy,
                                             instance_id,
                                             max_id_range,
                                             aws_region)
    sequence_id = int(sequence_generator.generate())
    print(sequence_id)
