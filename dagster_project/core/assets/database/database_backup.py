import subprocess
import datetime
import shutil
import os
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut, AssetExecutionContext
import time

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_BACKUP_USER')
password = os.getenv('SQL_BACKUP_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset(config_schema={'backup_directory': str,
                      'archive_days': int})
def database_backup_cleanup(context: AssetExecutionContext):
    backup_dir = context.op_config['backup_directory']
    days = context.op_config['archive_days']
    current_time = time.time()

    context.log.info(f'Cleaning up backups')
    file_names = os.listdir(backup_dir)
    for file_name in file_names:
        context.log.info(f'Moving File {file_name} to archive.')
        shutil.move(os.path.join(backup_dir, file_name), backup_dir + '/archive/')
    context.log.info(f'Cleaning up backups finished')

    context.log.info(f'Cleaning up archive')
    archive_file_names = os.listdir(backup_dir + '/archive/')
    for i in archive_file_names:
        file_location = os.path.join(backup_dir + '/archive/', i)
        file_time = os.stat(file_location).st_mtime
        if file_time < (current_time - 86400 * days):
            context.log.info(f'deleting {i} from archive.')
            os.remove(file_location)
    context.log.info(f'Cleaning up archive finished')

    return Output(value=backup_dir,
                  metadata={'backup_directory': backup_dir})


@asset(config_schema={'type': str})
def database_backup(context: AssetExecutionContext,
                    database_backup_cleanup: str):
    backup_dir = database_backup_cleanup
    type = context.op_config['type']

    if type not in ['manual', 'auto']:
        raise Exception('Invalid backup type')

    if not os.path.exists(backup_dir):
        raise Exception(f"Error: Backup directory '{backup_dir}' does not exist.")

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_file = f"{backup_dir}{type}_mysql_{timestamp}.sql"

    dump_command = f'mysqldump -h"{server}" -P{port} -u"{user}" -p"{password}" --all-databases --no-tablespaces > "{backup_file}"'

    context.log.info('Creating Backup')
    subprocess.run(dump_command, shell=True)
    context.log.info('Backup Done')

    if not os.path.isfile(backup_file):
        raise Exception(f"Error: Backup file '{backup_file}' has not been created.")

    return Output(value=backup_file,
                  metadata={
                      'backup_file': backup_file,
                      'timestamp': timestamp
                  }
    )
