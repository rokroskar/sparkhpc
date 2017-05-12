import pytest
import os
import sparkhpc 

def find_bindir():
    for root, subdirs, files in os.walk('.'):
        if root.endswith('bin'):
            return root

bindir = find_bindir()
testdir = os.path.join(bindir, '../')

os.environ['PATH'] = bindir + ':' + os.environ['PATH']

log_string = """
INFO:sparkhpc.sparkjob:master command: /cluster/home/roskarr/spark/sbin/start-master.sh
INFO:sparkhpc.sparkjob:[start_cluster] master running at spark://1.1.1.1:7077
INFO:sparkhpc.sparkjob:[start_cluster] master UI available at http://1.1.1.1:8080"""

@pytest.fixture(autouse=True)
def change_homedir(monkeypatch):
    monkeypatch.setattr(os.path, 'expanduser', lambda user: testdir)
    monkeypatch.setattr(sparkhpc.sparkjob, 'home_dir', testdir)

@pytest.fixture(params=['lsf','slurm'])
def sj(request):
    if request.param == 'lsf':
        sj = sparkhpc.LSFSparkJob()    
        yield sj
    elif request.param == 'slurm':
        sj = sparkhpc.SLURMSparkJob()

        # write a fake log so our _peek function finds it
        with open(os.path.join(os.getcwd(), 'sparkcluster-1.log'), 'w') as f:
            f.write(log_string)
        yield sj
        os.remove(os.path.join(os.getcwd(), 'sparkcluster-1.log'))

    else:
        raise RuntimeError('unknown scheduler')
    
    os.remove(os.path.join(testdir,'.sparkhpc1'))
    
def test_job_submission(sj):
    clusterid = sj.submit()
    assert(os.path.exists(os.path.join(testdir,'.sparkhpc%s'%sj.jobid)))
    assert(clusterid==0)

def test_finding_master(sj):
    sj.submit()
    assert(sj.master_ui() == 'http://1.1.1.1:8080')
    assert(sj.master_url() == 'spark://1.1.1.1:7077')




