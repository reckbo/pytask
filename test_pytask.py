from plumbum import local

from pytasks import ExternalTask, Pipeline, TaskGenerator


@TaskGenerator
def create_text_file(contents, output):
    output.write(contents)


@TaskGenerator
def hello(filepath, output):
    inputs = filepath.read()
    output.write('hello ' + inputs)


def make_pipeline(working_dir):
    with Pipeline(working_dir) as pipeline:
        source = create_text_file(contents='world', output='source.txt')
        hello(filepath=source, output='out/hello.txt')
    return pipeline


def test_pipeline_first_run():
    with local.tempdir() as tmpdir:
        pipeline = make_pipeline(tmpdir)
        source_txt = tmpdir / 'source.txt'
        hello_txt = tmpdir / 'out/hello.txt'
        assert not source_txt.exists()
        assert not hello_txt.exists()
        pipeline.run()
        assert source_txt.read() == 'world'
        assert hello_txt.read() == 'hello world'


def test_pipeline_creates_missing():
    with local.tempdir() as tmpdir:
        pipeline = make_pipeline(tmpdir)
        source_txt = tmpdir / 'source.txt'
        hello_txt = tmpdir / 'out/hello.txt'
        pipeline.run()
        source_txt.delete()
        pipeline.run()
        assert source_txt.read() == 'world'
        assert hello_txt.read() == 'hello world'


def test_pipeline_with_external_task():
    with local.tempdir() as tmpdir:
        with Pipeline(tmpdir) as pipeline:
            ExternalTask('external.txt')
        pipeline.run()


def test_pipeline_no_duplicate_outputs():
    with Pipeline() as pipeline:
        create_text_file(contents='world', output='source.txt')
        create_text_file(contents='world', output='source.txt')

    assert len(pipeline.tasks) == 1
