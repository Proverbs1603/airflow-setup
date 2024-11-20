{
          'table': 'mau_summary',
          'schema': 'tlsdnd1667',
          'main_sql': """SELECT 
                        TO_CHAR(A.ts, 'YYYY-MM') AS month,
                        COUNT(DISTINCT B.userid) AS mau
                      FROM raw_data.session_timestamp A
                      JOIN raw_data.user_session_channel B ON A.sessionid = B.sessionid
                      GROUP BY 1 
                      ;""",
          'input_check':
          [
            {
              # 'sql': 'SELECT COUNT(1) FROM raw_data.session_timestamp',
              # 'count': 12
            },
          ],
          'output_check':
          [
            {
              # 'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
              # 'count': 12
            }
          ],
}
