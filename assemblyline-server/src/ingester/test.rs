
def test_ingest_always_create_submission(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    # Simulate configuration where we'll always create a submission
    ingester.config.core.ingester.always_create = True
    get_if_exists = datastore.filescore.get_if_exists
    try:
        # Add a valid file score for all files
        from assemblyline.odm.models.filescore import FileScore
        from assemblyline.odm.models.submission import Submission
        datastore.filescore.get_if_exists = mock.MagicMock(
            return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='001', time=time.time()))
        )
        # Create a submission for cache hit
        old_sub = random_minimal_obj(Submission)
        old_sub.sid = '001'
        old_sub.params.psid = '000'
        old_sub = old_sub.as_primitives()
        datastore.submission.save('001', old_sub)

        # Ingest a file
        submission_msg = make_message(message={'sid': '002', 'metadata': {'blah': 'blah'}})
        submission_msg['sid'] = '002'
        in_queue.push(submission_msg)
        ingester.handle_ingest()

        # No file has made it into the internal buffer => cache hit and drop
        datastore.filescore.get_if_exists.assert_called_once()
        ingester.counter.increment.assert_any_call('cache_hit')
        ingester.counter.increment.assert_any_call('duplicates')
        assert ingester.unique_queue.length() == 0
        assert ingester.ingest_queue.length() == 0

        # Check to see if new submission was created
        new_sub = datastore.submission.get_if_exists('002', as_obj=False)
        assert new_sub and new_sub['params']['psid'] == old_sub['sid']

        # Check to see if certain properties are same (anything relating to analysis)
        assert all([old_sub.get(attr) == new_sub.get(attr) \
                    for attr in ['error_count', 'errors', 'file_count', 'files', 'max_score', 'results', 'state', 'verdict']])

        # Check to see if certain properties are different
        # (anything that isn't related to analysis but can be set at submission time)
        assert all([old_sub.get(attr) != new_sub.get(attr) \
                    for attr in ['expiry_ts', 'metadata', 'params', 'times']])

        # Check to see if certain properties have been nullified
        # (properties that are set outside of submission)
        assert not all([new_sub.get(attr) \
                        for attr in ['archived', 'archive_ts', 'to_be_deleted', 'from_archive']])
    finally:
        datastore.filescore.get_if_exists = get_if_exists