SELECT ownr_nm AS sb_nm
     , tbl_id AS tbl_nm
     , count(1) AS cnt
     , max(cnct_dttm) AS last_dttm
  FROM {@target_table}
 WHERE 1=1 
   AND cnct_dttm > {@start_time}
   AND db_nm = 'OBASEDB'
   AND (ownr_nm like 'br%' or ownr_nm like 'pj%')
   AND crud = 'R'
 GROUP BY 1,2
 ORDER BY sb_nm, tbl_id