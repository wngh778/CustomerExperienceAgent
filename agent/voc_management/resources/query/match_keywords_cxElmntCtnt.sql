SELECT  
  v73.문항응답내용, 
  g57.서비스품질요소코드,  
  g82.과제검토구분, 
  g82.과제검토의견내용, 
  g82.작성년월일시, 
  g82.과제추진사업내용, 
  g82.개선이행시작년월일, 
  g82.개선이행종료년월일 
FROM inst1.TSCCVMG82 AS g82 
JOIN inst1.TSCCVMG84 AS g84 
  ON g82.개선조치검토ID = g84.개선조치검토ID 
JOIN inst1.vsccvsv73 AS v73 
  ON g82.설문ID = v73.설문ID 
  AND g84.문항ID = v73.문항ID 
  AND g84.설문참여대상자고유ID = v73.설문참여대상자고유ID 
  AND v73.문항선택항목ID = '' 
  AND v73.설문응답종료년월일 >= DATE_SUB(NOW(), INTERVAL 1 YEAR) 
  AND v73.그룹회사코드 = 'KB0' 
JOIN inst1.tsccvmg57 AS g57 
  ON g57.설문ID = v73.설문ID 
  AND g57.문항ID = v73.문항ID 
  AND g57.설문참여대상자고유ID = v73.설문참여대상자고유ID 
WHERE v73.`문항응답내용` REGEXP '{pattern}'
  AND g57.`서비스품질요소코드` REGEXP '{cx_regex}'
ORDER BY g82.작성년월일시 DESC
LIMIT 500