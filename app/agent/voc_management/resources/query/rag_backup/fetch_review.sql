SELECT 개선조치검토ID, 과제검토구분, 작성년월일시
FROM inst1.TSCCVMG82
WHERE 개선조치검토ID IN ({review_ids_str})