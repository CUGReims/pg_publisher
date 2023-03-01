def test_logger_insert_row_publish_fail(dst_conn):
    from reims_publisher.core.logger import PublisherLogger

    logger = PublisherLogger(dst_conn)
    logger.object_type = "schemas"
    logger.insert_log_row()
    with dst_conn.cursor() as cursor:
        cursor.execute("SELECT succes, utilisateur FROM logging.logging;")
        row = cursor.fetchone()
        assert row == (False, "None")
        cursor.execute("TRUNCATE logging.logging;")


def test_logger_insert_row_publish_success(dst_conn):
    from reims_publisher.core.logger import PublisherLogger

    logger = PublisherLogger(dst_conn)
    logger.success = True
    logger.object_type = "schemas"
    logger.insert_log_row()
    with dst_conn.cursor() as cursor:
        cursor.execute("SELECT succes, utilisateur FROM logging.logging;")
        row = cursor.fetchone()
        assert row == (True, "None")
        cursor.execute("TRUNCATE logging.logging;")
