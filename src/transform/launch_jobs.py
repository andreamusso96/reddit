from typing import List
import os


def extract_zst_file_names(folder_path: str) -> List[str]:
    file_names = os.listdir(folder_path)
    file_names = [f for f in file_names if f.endswith('.zst')]
    return file_names

def _launch_jobs(zst_folder_path: str, parquet_folder_path: str, cluster: bool, time_str: str):
    file_names = extract_zst_file_names(folder_path=zst_folder_path)
    file_names = sorted(file_names)
    file_names = file_names[-2:] # TODO: remove this after testing
    bash_script_path = os.path.join(os.path.dirname(__file__), 'run_job.sh')
    os.system(f'chmod +x {bash_script_path}')
    for file_name in file_names:
        zst_file_path = os.path.join(zst_folder_path, file_name)
        if cluster:
            os.system(f'sbatch --time={time_str} {bash_script_path} {zst_file_path} {parquet_folder_path}')
        else:
            os.system(f'sh {bash_script_path} {zst_file_path} {parquet_folder_path}')


def launch_submissions_jobs(cluster: bool):
    if cluster:
        zst_folder_path = '/cluster/work/coss/anmusso/reddit/submissions'
        parquet_folder_path = '/cluster/work/gess/coss/anmusso/reddit_parquet/submissions'
    else:
        zst_folder_path = '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/data_trial/raw_submissions'
        parquet_folder_path = '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/data_trial/raw_submissions/parquet'

    print(f'Launching submissions jobs on {cluster}')
    print(f'ZST folder path: {zst_folder_path}')
    print(f'Parquet folder path: {parquet_folder_path}')
    _launch_jobs(zst_folder_path=zst_folder_path, parquet_folder_path=parquet_folder_path, cluster=cluster, time_str='01:00:00')


def launch_comments_jobs(cluster: bool):
    if cluster:
        zst_folder_path = '/cluster/work/coss/anmusso/reddit/comments'
        parquet_folder_path = '/cluster/work/gess/coss/anmusso/reddit_parquet/comments'
    else:
        zst_folder_path = '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/data_trial/raw_comments'
        parquet_folder_path = '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/data_trial/raw_comments/parquet'

    print(f'Launching comments jobs on {cluster}')
    print(f'ZST folder path: {zst_folder_path}')
    print(f'Parquet folder path: {parquet_folder_path}')
    _launch_jobs(zst_folder_path=zst_folder_path, parquet_folder_path=parquet_folder_path, cluster=cluster, time_str='03:30:00')


def launch_jobs(cluster: bool):
    launch_submissions_jobs(cluster=cluster)
    launch_comments_jobs(cluster=cluster)

if __name__ == '__main__':
    is_cluster = os.getcwd().startswith('/cluster')
    print(f'Launching jobs on {is_cluster}')
    launch_jobs(cluster=is_cluster)