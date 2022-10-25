def test_logger_insert_row(dst_conn):
    from reims_publisher.core.logger import PublisherLogger

    logger = PublisherLogger(dst_conn)
    logger.insert_log_row()
    with dst_conn.cursor() as cursor:
        cursor.execute("SELECT succes, utilisateur FROM logging.logging;")
        row = cursor.fetchone()
        assert row == (False, "None")
