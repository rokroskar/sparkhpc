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
def sj(monkeypatch, request):
    scheduler = request.param

    if scheduler == 'slurm':
        # for slurm, write a fake log so our _peek function finds it
        with open(os.path.join(os.getcwd(), 'sparkcluster-1.log'), 'w') as f:
            f.write(log_string)

    monkeypatch.setenv('PATH', os.path.join(bindir, scheduler), prepend=':')

    # we have to do this by hand because on import the paths to scheduler functions were not defined
    monkeypatch.setattr(sparkhpc.sparkjob, 'sparkjob', sparkhpc.sparkjob._sparkjob_factory(scheduler))

    sj = sparkhpc.sparkjob.sparkjob()    
    yield sj

    if scheduler == 'slurm':
        os.remove(os.path.join(os.getcwd(), 'sparkcluster-1.log'))
    
    try: 
        os.remove(os.path.join(testdir,'.sparkhpc1'))
    except FileNotFoundError:
        pass
    
def test_job_submission(sj):
    clusterid = sj.submit()
    assert(sj.jobid == '1')
    assert(os.path.exists(os.path.join(testdir,'.sparkhpc%s'%sj.jobid)))
    assert(clusterid==0)


def test_job_submission_fail(monkeypatch, sj):
    monkeypatch.setattr(sparkhpc.LSFSparkJob, '_job_regex', 'woop')
    monkeypatch.setattr(sparkhpc.SLURMSparkJob, '_job_regex', 'woop')
    
    with pytest.raises(IndexError):        
        sj.submit()


def test_wait_to_start(sj):
    sj.wait_to_start()
    assert(sj.job_started())


def test_attribute_retrieval(sj):
    sj.submit()
    assert(sj.jobid == '1')
    with pytest.raises(AttributeError): 
        sj.bad_attribute


def test_finding_master(sj):
    sj.submit()
    assert(sj.master_ui() == 'http://1.1.1.1:8080')
    assert(sj.master_url() == 'spark://1.1.1.1:7077')


def test_clusterid_start(sj): 
    sj.submit()
    sj2 = sj.__class__(clusterid=0)
    assert(sj.master_ui()) == 'http://1.1.1.1:8080'

    # this cluster doesn't exist
    with pytest.raises(RuntimeError):
        sj2 = sparkhpc.sparkjob.sparkjob(clusterid=100)


def test_jobid_start(sj): 
    sj.submit()

    # this should work
    sj2 = sj.__class__(jobid=1)
    assert(sj.master_ui()) == 'http://1.1.1.1:8080'

    # this should fail
    with pytest.raises(FileNotFoundError):
        sj2 = sj.__class__(jobid=100)


def test_job_started(sj):
    sj.submit()
    assert(sj.job_started() == True)


def test_current_clusters(sj): 
    sj.submit()
    assert(len(sj.current_clusters()) == 1)


def test_pretty_prints(sj, monkeypatch):
    sj.submit()
    sparkhpc.show_clusters()
    from IPython.display import display, HTML

    monkeypatch.setattr(sparkhpc.sparkjob, 'display', display, raising=False)
    monkeypatch.setattr(sparkhpc.sparkjob, 'HTML', HTML, raising=False)
    monkeypatch.setattr(sparkhpc.sparkjob, 'IPYTHON', True)

    sj.show_clusters()
    