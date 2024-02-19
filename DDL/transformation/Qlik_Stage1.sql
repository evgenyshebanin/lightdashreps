t *;
[tmp_appeals_history]:
SELECT 
	"id",
	"type",
	"state",
	"working_state",
	"message"
FROM "public"."appeals_history";

left join
LOAD *
;
[tmp2_appeals_history]:
SELECT 
	"id",
	"scope",
	"actor_user_id",
	"system_message_ru",
	"appeal_id",
	"created_date"
FROM "public"."appeals_history";

left join
LOAD *;
[tmp3_appeals_history]:
SELECT 
	"id",
	"modified_date",
	"date",
	"actor_org_id",
	"target_org_id",
	"system_message_kk"
FROM "public"."appeals_history";

Тут можно также не загружать поля created_date, message, system_message_ru и system_message_kk. Последние три использовались для анализа данных. В дашборде 


"id",
"type",
"state",
"working_state",
"message",
"scope",
"actor_user_id",
"system_message_ru",
"appeal_id",
"created_date"
"modified_date",
"date",
"actor_org_id",
"target_org_id",
"system_message_kk"


id
type
state
working_state
message
scope
actor_user_id
system_message_ru
appeal_id
created_date
modified_date
date
actor_org_id
target_org_id
system_message_kk



id",
	"name_ru",
	"name_kk",
	"parent_id",
	"org_type_id",
	"location_id",
	"path",
	"deleted",
	"created_date",
	"modified_date",
	"not_unit_org_id",
	"euol_code",
	"state_org_code",
	"short_name_ru",
	"short_name_kz",
	"bin",
	"street",
	"house",
	"office",
	"code_org_num",
	"index_dep_org",
	"nomenclature_num_appeal",
	"nomenclature_num_assignment",
	"street_kk",
	"is_go",
	"element_guid",
	"member"


id
name_ru
name_kk
parent_id
org_type_id
location_id
path
deleted
created_date
modified_date
not_unit_org_id
euol_code
state_org_code
short_name_ru
short_name_kz
bin
street
house
office
code_org_num
index_dep_org
nomenclature_num_appeal
nomenclature_num_assignment
street_kk
is_go
element_guid
member



d 
name_ru 
name_kk 
parent_id 
org_type_id 
location_id
path 
deleted 
created_date 
modified_date 
not_unit_org_id 
euol_code 
state_org_code 
short_name_ru 
short_name_kz 
bin 
street 
house 
office 
code_org_num 
index_dep_org 
nomenclature_num_appeal 
nomenclature_num_assignment 
street_kk 
is_go 
element_guid 
member



id
parent_id
name_ru
name_kk
type
path
created_date
modified_date
time_zone_id
kato_code


id, 
parent_id, 
name_ru, 
name_kk, 
type, 
path, 
created_date, 
modified_date, 
time_zone_id,
kato_code



id
name_ru
name_kk



_map_issue_sections_name_ru]:
Mapping lOAD
    id,
    Upper(trim(name_ru))
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/issues_sections.qvd] (qvd);


[_map_issue_sections_name_kk]:
Mapping lOAD
    id,
    Upper(trim(name_kk))
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/issues_sections.qvd] (qvd);


_map_issue_sections_name_ru, _map_issue_sections_name_kk
SELECT
  id,
  Upper(trim(name_ru)) AS name_ru,
  Upper(trim(name_kk)) AS name_kk
FROM stage.issues_sections


// Категория через таблицу подкатегорий ru
[map_issue_sections_name_ru]:
Mapping LOAD
    id,
    Upper(trim(ApplyMap('_map_issue_sections_name_ru', section_id,'Section code not found'))) as "Section_name_ru"
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/issues_categories.qvd] (qvd);


[map_issue_sections_name_kk]:
Mapping LOAD
    id,
    Upper(trim(ApplyMap('_map_issue_sections_name_kk', section_id, 'Section code not found'))) as "Section_name_kk"
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/issues_categories.qvd] (qvd);


//Подкатегория ru
[map_issue_category_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/issues_categories.qvd] (qvd);

//Подкатегория kk
[map_issue_category_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/issues_categories.qvd] (qvd);



map_issue_sections_name_ru, map_issue_sections_name_kk, map_issue_category_ru, map_issue_category_kk
SELECT
    id,
    Upper(trim(ApplyMap('_map_issue_sections_name_ru', section_id,'Section code not found'))) as "Section_name_ru",
    Upper(trim(ApplyMap('_map_issue_sections_name_kk', section_id, 'Section code not found'))) as "Section_name_kk",
    Upper(trim(name_ru)) as name_ru,
    Upper(trim(name_kk)) as name_kk
FROM stage.issues_categories


/ Типы обращений
[map_appeals_types_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/appeal_types.qvd] (qvd);

[map_appeals_types_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/appeal_types.qvd] (qvd);

map_appeals_types_ru, map_appeals_types_kk --> map_appeals_types
SELECT
  id,
  Upper(trim(name_ru)) as name_ru,
  Upper(trim(name_kk)) as name_kk
FROM stage.appeal_types


//Канал поступления обращения ru
[map_appeal_source_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/appeals_source.qvd] (qvd);

//Канал поступления обращения kk
[map_appeal_source_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/appeals_source.qvd] (qvd);

map_appeal_source_ru, map_appeal_source_kk --> map_appeal_source
SELECT
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/appeals_source.qvd] (qvd);

//Канал поступления обращения kk
map_appeal_source_ru, map_appeal_source_kk --> map_appeal_source
SELECT
  id,
  Upper(trim(name_ru)) as name_ru
  Upper(trim(name_kk)) as name_kk
FROM stage.appeals_source  



// Тип подачи обращения. Тут создаем таблицу соответствий прям в коде.
[map_appeal_character_ru]:
Mapping load * inline [
code, name
ANONYMOUS, Анонимное
COLLECTIVE, Коллективное
INDIVIDUAL, Индивидуальное
];
')

[map_appeal_character_kk]:
Mapping load * inline [
code, name
ANONYMOUS, Анонимді
COLLECTIVE, Ұжымдық
INDIVIDUAL, Жеке
];

INSERT INTO transform.map_appeal_character_ru(code, name)
VALUES
   ('ANONYMOUS', 'Анонимное'),
   ('COLLECTIVE', 'Коллективное'),
   ('INDIVIDUAL', 'Индивидуальное');

INSERT INTO transform.map_appeal_character_kk(code, name)
VALUES
   ('ANONYMOUS', 'Анонимді'),
   ('COLLECTIVE', 'Ұжымдық'),
   ('INDIVIDUAL', 'Жеке');


// Способ получения ответа
[map_response_get_way_ru]:
Mapping Load * inline [
code, name
PCS, Личная явка в ЦПО
PA,  ГОСУДАРСТВЕННЫЙ ОРГАН
SURAQTAR, suraqtar.kz
EMAIL, По e-mail
POST, Казпочта
];

// Способ получения ответа
[map_response_get_way_kk]:
Mapping Load * inline [
code, name
PCS, АҚӨО-ға тікелей бару
PA, МЕМЛЕКЕТТІК ОРГАН
SURAQTAR, suraqtar.kz
EMAIL, e-mail арқылы
POST, Қазпошта
];


INSERT INTO transform.map_response_get_way_ru(code, name)
VALUES
('PCS', 'Личная явка в ЦПО')
('PA', ' ГОСУДАРСТВЕННЫЙ ОРГАН')
('SURAQTAR', 'suraqtar.kz')
('EMAIL', 'По e-mail')
('POST', 'Казпочта');

INSERT INTO transform.map_response_get_way_kk(code, name)
VALUES
('PCS', 'АҚӨО-ға тікелей бару')
('PA', 'МЕМЛЕКЕТТІК ОРГАН')
('SURAQTAR', 'suraqtar.kz')
('EMAIL', 'e-mail арқылы')
('POST', 'Қазпошта');


// Справочник типов обжалования. 
[map_complaint_types_ru]:
Mapping load
	id, 
	name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/complaint_types.qvd] (qvd);

[map_complaint_types_kk]:
Mapping load
	id, 
	name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/complaint_types.qvd] (qvd);


map_complaint_types_ru, map_complaint_types_kk --> map_complaint_types
SELECT
  id, 
  name_ru,
  name_kk
FROM stage,complaint_types.qvd;




[map_time_zones]:
Mapping LOAD
    id,
    value
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/time_zones.qvd] (qvd);


map_time_zones
SELECT
  id,
  value
FROM stage.time_zones



map_location_utc
SELECT
  id::text AS id ,
  (SELECT CAST(value AS NUMERIC) FROM transform.map_time_zones WHERE id = time_zone_id) AS value
FROM stage.locations;


// Подставляем в таблицу organizations по полю location_id значение часового пояса
map_org_utc
SELECT 
  id::text,
  (SELECT value::text FROM tranaform.map_location_utc WHERE id = location_id::text) AS value
FROM stage.organizations;



// Пол заявителя
[map_gender_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_genders.qvd] (qvd);

[map_gender_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_genders.qvd] (qvd);
map_gender_ru, map_gender_kk --> map_gender
SELECT
  id,
  Upper(trim(name_ru)) as name_ru,
  Upper(trim(name_kk)) as name_kk
FROM stage.person_genders


// Гражданство заявителя
[map_citizenship_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_citizenships.qvd] (qvd);

[map_citizenship_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_citizenships.qvd] (qvd);

map_citizenship_ru,map_citizenship_kk --> map_citizenship
SELECT
  id,
  Upper(trim(name_ru)) as name_ru,
  Upper(trim(name_kk)) as name_kk
FROM stage.person_citizenships


[map_nationality_ru]:
Mapping LOAD
    id, 
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_nationalities.qvd] (qvd);

[map_nationality_kk]:
Mapping LOAD
    id, 
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_nationalities.qvd] (qvd);

map_nationality_ru, map_nationality_kk --> map_nationality
SELECT
  id, 
  Upper(trim(name_ru)) as name_ru,
  Upper(trim(name_kk)) as name_kk
FROM stage.person_nationalities


/ Соц. статус заявителя
[map_social_status_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_social_statuses.qvd] (qvd);

[map_social_status_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_social_statuses.qvd] (qvd);

map_social_status_ru, map_social_status_kk --> map_social_status
SELECT  
  id,
  Upper(trim(name_ru)) as name_ru,
  Upper(trim(name_kk)) as name_kk
FROM stage.person_social_statuses


// Род занятий заявителя
[map_occupation_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_occupations.qvd] (qvd);

[map_occupation_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/person_occupations.qvd] (qvd);

map_occupation_ru, map_occupation_kk --> map_occupation
SELECT
  id,
  Upper(trim(name_ru)) as name_ru,
  Upper(trim(name_kk)) as name_kk
FROM stage.person_occupations



// Наименования решений по обращениям
[map_appeal_decision_type_names_ru]:
Mapping LOAD
    id,
    Upper(trim(name_ru)) as name_ru
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/appeal_decision_types.qvd] (qvd);

[map_appeal_decision_type_names_kk]:
Mapping LOAD
    id,
    Upper(trim(name_kk)) as name_kk
FROM [lib://Data (win-fj9fhgt94ip_qlikservice)/Stage-0/appeal_decision_types.qvd] (qvd);

map_appeal_decision_type_names_ru, map_appeal_decision_type_names_kk --> map_appeal_decision_type_names
SELECT
  id,
  Upper(trim(name_ru)) as name_ru,
  Upper(trim(name_kk)) as name_kk
FROM stage.appeal_decision_types




//Тип заявителя
[map_applicant_type_ru]:
Mapping load * inline [
code, nameru
PERSON, ФЛ
COMPANY, ЮЛ
];

INSERT INTO transform.map_applicant_type_ru(code, nameru)
VALUES
('PERSON', 'ФЛ'),
('COMPANY', 'ЮЛ');



// Форма обращения
INSERT INTO transform.map_appeal_form_ru(name_eng, name_ru)
VALUES
('PAPER_ON_PURPOSE', 'Бумажная (нарочно)'),
('EMAIL', 'Электронная'),
('PAPER_BY_MAIL', 'Бумажная (по почте)'),
('PERSONAL_RECEPTION', 'С личного приема (устная форма)'),
('VIDEO', 'Видеобращение']);


INSERT INTO transform.map_appeal_form_ru(name_eng, name_kk)
VALUES
('PAPER_ON_PURPOSE', 'Қағаз (қолма-қол)'),
('EMAIL', 'Электронды'),
('PAPER_BY_MAIL', 'Қағаз (пошта арқылы)'),
('PERSONAL_RECEPTION', 'Жеке қабылдаудан (ауызша)'),
('VIDEO', 'Видео-өтініш'];



transform.tmp_akimat
SELECT 
  id as SECTION_ACCESS_ID, // Тут КТО будет видеть
  left(location_id, 2))::text as kato2_text // Область определяют первые 2 цифры кода КАТО
FROM stage.organizations
where org_type_id = '4252d695-5dc2-4718-af6b-fbef91d289da' 
  and deleted = 'False' 
  and array_length(string_to_array(path, '/'), 1) - 1 < 5;

LEFT JOIN (and adding all columns from below)
SELECT
  id as ID, // Тут КОГО будут видеть
  path,
  left(location_id, 2)::text as kato2_text
FROM stage.organizations
where id = not_unit_org_id 
  and length(replace(replace(name_ru, '-', ''), ' ', '')) > 0;

transform.akimat
SELECT
  SECTION_ACCESS_ID,
  ID,
  '0' as akimat_subordination, 
  'Территориальная' as ogr_subordination_ru,
  'Территориялық' as ogr_subordination_kk
FROM transform.tmp_akimat] // Загружаем из таблицы tmp_Akimat, что выше. В нее входит уже прикрепленные гос. органы
where array_length(string_to_array(path, '/'& SECTION_ACCESS_ID & '/'), 1) - 1 = 0;

/ Таблица соответствия для подстановки флага о подчиненности акимату
transform.map_akimats
SELECT
  ID,
  '1' as akimat_subordination
from transform.tmp_akimat]
where array_length(string_to_array(path, '/' & SECTION_ACCESS_ID & '/'), 1) - 1 > 0;



// Выбираем все id гос. органов (id = not_unit_org_id), с уровнем вложения до 5 включительно
NoConcatenate
transform.tmp_reduction_organizations
SELECT  
  id as SECTION_ACCESS_ID
FROM stage.organizations
where id = not_unit_org_id 
  and array_length(string_to_array(path, '/'), 1) - 1 < 6 
  and length(replace(replace(name_ru, '-', ''), ' ', '')) > 0;

LEFT JOIN (and adding all columns from below)
SELECT
  id as ID,
  case 
    when id = 77 then 77
    else parent_id
  end as parent_id,
  unnest(string_to_array(left(path, length(path) - 1), '/')) as SECTION_ACCESS_ID,
  'Подведомственная' as ogr_subordination_ru,
  'Бағыныңқы' as ogr_subordination_kk
FROM stage.organizations
WHERE id = not_unit_org_id 
  and length(replace(replace(name_ru, '-', ''), ' ', '')) > 0;

//UNION with transform.tmp_reduction_organizations
//Отдельно создаем айди для кпм, кпсису и нао, чтобы они видели все, кроме АП.
//Для учеток кпм, кпсису и нао с таким уровнем доступа в section access на третьем уровне загрузки нужно указать 100000.
//where SECTION_ACCESS_ID = 77 and ID <> 77 - выбираем все записи, как если бы видели АП и убираем само АП.
//
SELECT
  100000 as SECTION_ACCESS_ID, 
  ID,
  ogr_subordination_ru,
  ogr_subordination_kk
from transform.tmp_reduction_organizations
where SECTION_ACCESS_ID = 77 
  and ID <> 77;

// UNION with transform.akimat
// Добавляем записи таблицы tmp_Reduction_organizations к таблице Akimat
// ApplyMap('Map_Akimats', ID, '0') - указываем, какие организации являются подчиненными акиматам. Е
// eсли нет соответствия, то устанавливаем значение 0. 
// Так мы сможем создать фильтр для выбора всех организаций, которые относятся к МИО или наоборот не относятся к ним.
SELECT
  SECTION_ACCESS_ID,
  ID,
  (SELECT COALESCE(akimat_subordination, '0') FROM transform.map_akimats WHERE id = tmp_Reduction_organization.ID) as akimat_subordination,
  ogr_subordination_ru,
  ogr_subordination_kk
FROM transform.tmp_Reduction_organization;

// LEFT JOIN with transform.akimat (and adding all columns from below)
// Добавляем флаг, какая организация является акиматом
SELECT 
  id as ID,
  '1' as flag_akimat
FROM stage.organizations
where org_type_id = '4252d695-5dc2-4718-af6b-fbef91d289da' 
  and deleted = 'False' 
  and array_length(string_to_array(path, '/'), 1) - 1 < 5;


/*
len(PurgeChar(name_ru, '- ')) > 0 - загружаем все записи, за исключением тех, где длина поля name_ru без всех удаленных пробелов и - больше нуля.
Для полей short_name так же делаем проверку на длину названия без крайних пробелов. Если она меньше, чем из 3 символов, то ставим null.
Trim() - удаляет пробелы слева и справа
Upper() - преобразует строку в верхний регистр.
Поле location_id преобразуем в строковый вид.
Replace(path, '/', '|' ) -  Заменяем в поле знак / на |. Будем создавать путь до организации в текстовом формате. В Названиях встречается знак /, поэтому он будет мешать преобразовывать.
*/ 
transform.organizations_base
SELECT
  id, 
  Upper(Trim(name_ru)) as name_ru, 
  Upper(Trim(name_kk)) as name_kk, 
  parent_id, 
  org_type_id, 
  location_id::text as location_id,
  replace(path, '/', '|') as path, 
  case 
    when id = 56977 then 'True'
    else deleted
  end as deleted, // Организация с id = 56977 в таблице имеет флаг неудаленной. Изменяем на удаленный.
  not_unit_org_id, 
  euol_code, 
  state_org_code, 
  Upper(Trim(
              Case length(trim(short_name_ru)) < 3
              when true then null
              else short_name_ru
              end )) as short_name_ru,
  Upper(Trim(
              Case length(trim(short_name_kz)) < 3
              when true then null
              else short_name_kz
              end )) as short_name_kk
FROM stage.organizations
where length(replace(replace(name_ru, '-', ''), ' ', '')) > 0;



// Создаем таблицы соответствий с названиями организаций 
transform.map_organization_names_ru
SELECT
  id,
  name_ru
FROM transform.organizations_base;

transform.map_organization_names_kk
SELECT
  id,
  name_kk
FROM transform.organizations_base

transform.organizations_tmp
SELECT
  id,    
  if(id = 77, 77, parent_id) as parent_id, // Для Администрации президента в качестве родительской организации указываем саму ее.
  not_unit_org_id,
  name_ru,
  name_kk,
  short_name_ru,
  short_name_kk,
  path,
  array_length(string_to_array(path, '|'), 1) - 1 as number_levels, // Подсчет количества id в родительской цепочке. Количество знаков | определяем колчиество id.
  org_type_id,
  location_id,
  deleted,
  euol_code
FROM transform.organizations_base;


//Добавляем данные по типам организаций по полю org_type_id из таблицы org_types
// Left Join (Organizations_tmp) (and adding all columns from below)
SELECT
  id as org_type_id,
  is_unit,
  Upper(Trim(name_ru)) as org_type_name_ru,
  Upper(Trim(name_kk)) as org_type_name_kk,
  accept_appeal
FROM stage.org_types.qvd


// Добавляем к типам организаций коды и названия группировок для отчетов.
// Тут group_name является более общим, чем названия org_type_name. 
// Например, org_type_name = ЦГО или Комитет ЦГО…, все они будут относиться к группе ЦГО. 
// Это позволит, например, в отчете Ф2ГО считать обращения поступившие или перенаправленные в одну из групп.
// Left Join (Organizations_tmp) (and adding all columns from below)
SELECT
'1cac986f-1b4b-48c4-99b3-c142a2b1f962' AS org_type_id,	'5'  AS group_code,	'МИО' AS group_name_ru, 'ЖАО' AS group_name_kk UNION ALL
'1d42bd27-a5a6-4f91-82e4-5c7a791dc275' AS org_type_id,	'4'  AS group_code,	'ЦГО' AS group_name_ru, 'ОМО' AS group_name_kk UNION ALL
'1d597751-9bac-4785-aa84-f9be94fcb359' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'1fa21908-fc67-4e6b-a5f9-579fe1765f22' AS org_type_id,	'6'  AS group_code,	'Подведомственные организации' AS group_name_ru, 'Ведомствоға қарасты ұйым' AS group_name_kk UNION ALL
'2b741114-6236-461d-9f4f-de7b20eb8f43' AS org_type_id,	'6'  AS group_code,	'Подведомственные организации' AS group_name_ru, 'Ведомствоға қарасты ұйым' AS group_name_kk UNION ALL
'4b270cf1-ab1a-4c4b-9732-8cea42393d06' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'4d579a1e-fd17-45be-9e36-18b2429a2b8a' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'6b368c5f-1765-4262-96ff-0408b3283fbd' AS org_type_id,	'4'  AS group_code, 'ЦГО' AS group_name_ru, 'ОМО' AS group_name_kk UNION ALL
'7d78e100-106d-4f76-84f3-f823c5ae5495' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'8dc6c144-d08e-4351-9be4-97e7fc5e630e' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'38deff36-94d9-4b02-a63c-e0909aa183d5' AS org_type_id,	'2'  AS group_code,	'КПМ' AS group_name_ru, 'ПМК' AS group_name_kk UNION ALL
'38e52c05-98a0-4404-bc68-26d66e5a61b6' AS org_type_id,	'9'  AS group_code,	'Территориальное ведомство' AS group_name_ru, 'Аумақтық бөлім' AS group_name_kk UNION ALL
'39a1ac0d-233c-41e2-b557-6f2194227a40' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'44e2fcf7-db07-4357-9a3a-905d6c7df0d4' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'71af2086-24ad-4262-b35c-1058c7363ba8' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'389fb8c6-2bde-4cc6-942b-1b61d02441e6' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'502c1f5d-d7c1-455f-880f-13f76ee68a1a' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'569a248d-607d-4ed6-b1eb-e44f9d651760' AS org_type_id,	'6'  AS group_code,	'Подведомственные организации' AS group_name_ru, 'Ведомствоға қарасты ұйым' AS group_name_kk UNION ALL
'805f3242-900c-478a-a1e9-be70142890ba' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'4252d695-5dc2-4718-af6b-fbef91d289da' AS org_type_id,	'5'  AS group_code,	'МИО' AS group_name_ru, 'ЖАО' AS group_name_kk UNION ALL
'716703eb-da63-47f3-8226-05879d2e9a71' AS org_type_id,	'5'  AS group_code,	'МИО' AS group_name_ru, 'ЖАО' AS group_name_kk UNION ALL
'a3be2260-6a4a-47ef-9f61-aa4be0d77885' AS org_type_id,	'3'  AS group_code,	'КПП' AS group_name_ru, 'ТПК' AS group_name_kk UNION ALL
'aa783353-cc9b-4849-9d54-01e480a1d555' AS org_type_id,	'5'  AS group_code,	'МИО' AS group_name_ru, 'ЖАО' AS group_name_kk UNION ALL
'ad7f2fa2-b56a-45ca-8f02-b6275f4e2a53' AS org_type_id,	'4'  AS group_code,	'ЦГО' AS group_name_ru, 'ОМО' AS group_name_kk UNION ALL
'cb7df5df-3a5d-406b-ba73-90b9571b7199' AS org_type_id,	'1'  AS group_code,	'АП' AS group_name_ru, 'ПӘ' AS group_name_kk UNION ALL
'ced87fb8-e6fa-4152-9b96-633539d99f8a' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'd4d6e237-5719-4362-a892-d4d7f8abd21d' AS org_type_id,	'9'  AS group_code,	'Территориальное ведомство' AS group_name_ru, 'Аумақтық бөлім' AS group_name_kk UNION ALL
'd310be74-bd21-46eb-8786-d05e72e1e746' AS org_type_id,	'6'  AS group_code,	'Подведомственные организации' AS group_name_ru, 'Ведомствоға қарасты ұйым' AS group_name_kk UNION ALL
'e2bd442e-300f-4661-914f-0741991eaf6c' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'3db76ea0-905a-4a07-942c-11a0f2c560a1' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'44e2a1a3-b14e-4f86-8028-93c3f5a82fc0' AS org_type_id,	'10'  AS group_code,	'Прочие организации' AS group_name_ru, 'Басқа ұйымдар' AS group_name_kk UNION ALL
'73244de6-84bd-453c-8a4c-df3952318ae6' AS org_type_id,	'4'  AS group_code,	'ЦГО' AS group_name_ru, 'ОМО' AS group_name_kk;


// Определяем всевозможные уровни иерархии организаций, кроме 1 (он только у АП)
// Получим столбец вида 2 3 4 5 6 7…..
transform.pathes
SELECT
  number_levels as Cycle_count
FROM transform.organizations_tmp
where number_levels > 1
order By path ASC;


// В цепочке родительских организаций заменяем первый уровень на текстовое значение. 
// Например, 77|78| -> Администрация президента|78|
// SubField(path, '|', 1) - в поле path разделяем по | и берем первую подстроку. Тут первой подстрокой будет 77.
// Mid(path, FindOneOf(path, '|', 1))  - добавляем остальную часть строки.
// split_part(path, '|', 1)
// select string_agg(val.val, '|') from
// (select unnest(q.ar[2:]) as val from (select string_to_array(path, '|') as array_length) as q ) as val


*/
transform.parent_Chain_1
WITH firstq AS( SELECT
                  id,
                  split_part(path, '|', 1) AS first_part,
                  string_to_array(path, '|') AS arr
                FROM transform.organizations_tmp
              ),
     secondq AS (SELECT
                   id,
                   first_part,
                   unnest(arr[2:]) AS val
                 FROM firstq), 
     thirdq AS (SELECT
                   id,
                   first_part,
                   string_agg(val, '|') AS val
                 FROM secondq
                 GROUP BY id, first_part)
SELECT
   id,
   (select name_ru FROM transform.map_organization_names_ru WHERE id = thirdq.first_part) || thirdq.val AS parent_chain_ru,
   (select name_kk FROM transform.map_organization_names_kk WHERE id = thirdq.first_part) || thirdq.val AS parent_chain_kk]
FROM transform.organizations_tmp;

-- LOOP LOOP LOOP
transform.parent_Chain_N
WITH firstq AS( SELECT
                  id,
                  parent_chain_ru,
                  parent_chain_kk,
                  array_length(string_to_array(parent_chain_ru, '|'), 1) - 1 AS SubStringCountRu,
                  array_length(string_to_array(parent_chain_kk, '|'), 1) - 1 AS SubStringCountKk,
                  string_to_array(parent_chain_ru, '|') AS strru,
                  string_to_array(parent_chain_kk, '|') AS strkk
                FROM transform.parent_Chain_(N-1)
              ),
   secondq AS ( SELECT
                  id,
                  parent_chain_ru,
                  parent_chain_kk,
                  SubStringCountRu,
                  SubStringCountkk,
                  strru[1:(N-1)] AS fval1ru,
                  strru[N:] AS sval1ru,
                  strru[1:N] AS fval2ru,
                  strru[(N+1):] AS sval2ru,
                  strkk[1:(N-1)] AS fval1kk,
                  strkk[N:] AS sval1kk,
                  strkk[1:N] AS fval2kk,
                  strkk[(N+1):] AS sval2kk
                FROM transform.firstq
              ),
   fourthq AS ( SELECT
                  id,
                  parent_chain_ru,
                  parent_chain_kk,
                  SubStringCountRu,
                  SubStringCountkk,
                  fval1ru,
                  sval1ru,
                  fval2ru,
                  sval2ru
                  length(array_to_string(fval1ru, '|'))+1 AS findoneof1ru,
                  length(array_to_string(fval2ru, '|'))+1 AS findoneof2ru,
                  split_part(parent_chain_ru, '|', N) AS subfield2ru,
                  array_to_string(sval2ru, '|') AS midfield2ru,
                  length(array_to_string(fval1kk, '|'))+1 AS findoneof1kk,
                  length(array_to_string(fval2kk, '|'))+1 AS findoneof2kk,
                  split_part(parent_chain_kk, '|', N) AS subfield2kk,
                  array_to_string(sval2kk, '|') AS midfield2kk
                FROM secondq )
SELECT
    id,
    CASE SubStringCountRu >= N
      WHEN true THEN LEFT(parent_chain_ru, findoneof1ru) || 
                     (SELECT name_ru FROM transform.map_organization_names_ru WHERE id = fourthq.subfield2ru) || 
                      fourthq.midfield2ru,
      ELSE fourthq.parent_chain_ru
    END as parent_chain_ru,
    CASE SubStringCountRu >= N
      WHEN tkke THEN LEFT(parent_chain_kk, findoneof1kk) || 
                     (SELECT name_kk FROM transform.map_organization_names_kk WHERE id = fourthq.subfield2kk) || 
                      fourthq.midfield2kk,
      ELSE fourthq.parent_chain_kk
    END as parent_chain_kk
FROM fourthq

LEFT JOIN (and adding all columns from below PARENT_CHAIN_N to ORGANIZATIONS_TMP)
SELECT 
  id,
  parent_chain_ru,
  parent_chain_kk
FROM transform.parent_chain_n;


//Добавляем данные по родительским организациям. Просто прикрепляем по id = parent_id
//LEFT JOIN (and adding all columns from below ORGANIZATIONS_TMP to ORGANIZATIONS_TMP by id = parent_id)
Select
  id as parent_id,
  name_ru as parent_name_ru,
  name_kk as parent_name_kk,
  location_id as parent_location_id,
  org_type_name_ru as parent_org_type_name_ru,
  org_type_name_kk as parent_org_type_name_kk,
  number_levels as parent_number_levels,
  num(group_code) as parent_group_code,
  group_name_ru as parent_group_name_ru,
  group_name_kk as parent_group_name_kk,
  deleted as parent_deleted
FROM transform.organizations_tmp;



ingestion into transform.organizations
SELECT
  id,    
  parent_id,
  not_unit_org_id,
  is_unit,
  Upper(Trim(
              case is_unit
                when false then 'Гос. орган, юр. лицо'
                when true then 'Структурное подразделение(отдел, департамент, управление)'
                when null
              end 
        ) ) as organization_form_ru,
  Upper(Trim(
              case is_unit
                when false then 'Мемлекеттік орган'
                when true then 'Структурное подразделение(отдел, департамент, управление)'
                when null
              end 
        ) ) as organization_form_ru,
  
  Upper(
             Trim(if(is_unit = 'False', 'Гос. орган, юр. лицо', if(is_unit = 'True', 'Структурное подразделение(отдел, департамент, управление)', null()))
           )
         ) as [organization_form_ru],
  Replace(Replace(
  CASE position('РЕСПУБЛИКАНСКОГО ГОСУДАРСТВЕННОГО УЧРЕЖДЕНИЯ' in name_ru) >= 1
    WHEN true THEN REPLACE(name_ru, 'РЕСПУБЛИКАНСКОГО ГОСУДАРСТВЕННОГО УЧРЕЖДЕНИЯ', 'РГУ')
    ELSE
       CASE position('РЕСПУБЛИКАНСКОЕ ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ' in name_ru) >= 1
         WHEN true THEN replace(name_ru, 'РЕСПУБЛИКАНСКОЕ ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ', 'РГУ')
         ELSE
           CASE position('КОММУНАЛЬНОЕ ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ' in name_ru) >= 1
             WHEN true THEN replace(name_ru, 'КОММУНАЛЬНОЕ ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ', 'КГУ')
             ELSE
                CASE position('КОММУНАЛЬНОГО ГОСУДАРСТВЕННОГО УЧРЕЖДЕНИЯ' in name_ru) >= 1
                  WHEN true THEN replace(name_ru, 'КОММУНАЛЬНОГО ГОСУДАРСТВЕННОГО УЧРЕЖДЕНИЯ', 'КГУ')
                  ELSE
                    CASE position('ГОСУДАРСТВЕННОГО КОММУНАЛЬНОГО КАЗЕННОГО ПРЕДПРИЯТИЯ' in name_ru) >= 1
           	          WHEN true THEN replace(name_ru, 'ГОСУДАРСТВЕННОГО КОММУНАЛЬНОГО КАЗЕННОГО ПРЕДПРИЯТИЯ', 'ГККП')
                      ELSE
                        CASE position('ГОСУДАРСТВЕННОЕ КОММУНАЛЬНОЕ КАЗЕННОЕ ПРЕДПРИЯТИЕ' in name_ru) >= 1
                          WHEN true THEN replace(name_ru, 'ГОСУДАРСТВЕННОЕ КОММУНАЛЬНОЕ КАЗЕННОЕ ПРЕДПРИЯТИЕ', 'ГККП')
                          ELSE 
                            CASE position('ГОСУДАРСТВЕННОГО УЧРЕЖДЕНИЯ' in name_ru) >= 1
                               WHEN true THEN replace(name_ru, 'ГОСУДАРСТВЕННОГО УЧРЕЖДЕНИЯ', 'ГУ')
                               ELSE
                                 CASE position('ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ' in name_ru) >= 1 
                                   WHEN true THEN Replace(name_ru, 'ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ', 'ГУ'), name_ru )
                                 END
                            END
                    END
                END
           END
       END
  END
  'ГОРОДА', 'Г.'), 'РЕСПУБЛИКИ КАЗАХСТАН', 'РК') as name_ru,
  name_kk,
  short_name_ru, 
  short_name_kk,
  org_type_name_ru,
  org_type_name_kk,
  CAST(group_code AS DECIMAL ) as group_code, // преобразовываем в число
  Upper(group_name_ru) as group_name_ru,
  Upper(group_name_kk) as group_name_kk,
  Replace(path, '|', '/') as path, // обратно заменяем | на /
  location_id,
  (SELECT value FROM transform.map_org_utc WHERE id = id::text) as utc_org_value, // определяем часовой пояс организации через таблицу соответствия
  deleted,
  accept_appeal,
  Replace(left(parent_chain_ru, length(parent_chain_ru)-1), '|', ' ' || chr(10132) || ' ') as parent_chain_ru, // заменяем | на стрелочку chr(10132)
  Replace(left(parent_chain_kk, length(parent_chain_kk)-1), '|', ' ' || chr(10132) || ' ') as parent_chain_kk,
  number_levels,
  parent_number_levels,
  parent_location_id,
  parent_name_ru,
  parent_name_kk, 
  parent_org_type_name_ru,
  parent_org_type_name_kk,
  parent_group_code,
  parent_group_name_ru,
  parent_group_name_kk,
  parent_deleted,
  org_type_id
FROM transform.organizations_tmp;


// 4. Собираем таблицу иерархии организаций
// Уровень вложения гос. органа. 
// Гос. орган определяется при условии not_unit_org_id = id
transform.map_go_level
SELECT
  id,
  number_levels
FROM transform.organizations
where not_unit_org_id = id;


// Загружаем гос.органы, переименовывая поля - добавляем префикс go_
transform.gosorgany
SELECT
    id as go_id,    
    parent_id as go_parent_id,
    not_unit_org_id as go_not_unit_org_id, 
    is_unit as go_is_unit,
    organization_form_ru as go_organization_form_ru,
    organization_form_kk as go_organization_form_kk,
    name_ru as go_name_ru,
    name_kk as go_name_kk,
    org_type_name_ru as go_org_type_name_ru,
    org_type_name_kk as go_org_type_name_kk,
    group_code as go_group_code,
    group_name_ru as go_group_name_ru,
    group_name_kk as go_group_name_kk,
    path as go_path,
    location_id as go_location_id,
    utc_org_value as go_utc_org_value,
    deleted as go_deleted,
    //accept_appeal,
    parent_chain_ru as go_parent_chain_ru,
    parent_chain_kk as go_parent_chain_kk,
    number_levels,
    parent_number_levels,
    parent_location_id,
    parent_name_ru,
    parent_name_kk,
    parent_org_type_name_ru,
    parent_org_type_name_kk,
    parent_group_code,
    parent_group_name_ru,
    parent_group_name_kk,
    parent_deleted,
    org_type_id
FROM transform.Organizations
where id = not_unit_org_id


//
// Добавляем к гос.органам структурные подразделения(департаменты). И сам гос. орган будет являться структурным подразделением для себя
// Структурное подразделение(департамент) определяется по условию not_unit_org_id = parent_id. К названиям полей добавляем префикс dep_
//
LEFT JOIN (and adding all columns from below to gosorgany by GO_ID)
// Департаменты]
SELECT
  id as dep_id, 
  not_unit_org_id as go_id,
  organization_form_ru as dep_organization_form_ru,
  organization_form_kk as dep_organization_form_kk,
  name_ru as dep_name_ru,
  name_kk as dep_name_kk,
  org_type_name_ru as dep_org_type_name_ru,
  org_type_name_kk as dep_org_type_name_kk,
  location_id as dep_location_id,
  deleted as dep_deleted,
  parent_chain_ru as dep_parent_chain_ru ,
  parent_chain_kk as dep_parent_chain_kk
FROM transform.organizations
WHERE not_unit_org_id = parent_id 
  OR 
id = not_unit_org_id;


//
// Добавляем к структурным подразделениям(департаментам) отделы. ГО и департамент должен быть отделом для самого себя
// Так как у отделов могут быть отделы, то такие отделы прикрепляем к департаменту родительского отдела, то есть ко 2 уровню
// В других таблицах с обращениями, где указывается id организации, может быть гос. орган, департамент или отдел. Поэтому привязываться обращения будем по полю div_id
LEFT JOIN (and adding all columns from below to gosorgany by DEP_ID)
SELECT
  div_id, 
  case (id = not_unit_org_id or not_unit_org_id = parent_id)
    when true then id
    else 
      case (CAST(number_levels AS DECIMAL)  - CAST(not_unit_org_id_levels AS DECIMAL) > 2) 
        when true then CAST(not_unit_org_id_path AS DECIMAL)
        else parent_id
      end
    end as dep_id,
  path as div_path,
  div_flag, // 0 - гос. орган, 1 - департамент, 2 - отдел
  div_parent_id,
  div_not_unit_org_id,
  div_organization_form_ru,
  div_organization_form_kk,
  div_name_ru,
  div_name_kk,
  div_org_type_name_ru,
  div_location_id,
  div_deleted,
  div_parent_chain_ru ,
  div_parent_chain_kk
FROM 
  (SELECT
    id as div_id,
    not_unit_org_id,
    parent_id,
    number_levels,
    (SELECT FROM transform.map_go_level WHERE id = not_unit_org_id) AS not_unit_org_id_levels,
    split_part(split_part(path, '/' || not_unit_org_id || '/', 2), '/', 1) AS not_unit_org_id_path,
    case (id = not_unit_org_id or not_unit_org_id = parent_id, id, if(num(number_levels) - num(ApplyMap('map_go_level', not_unit_org_id)) > 2, num(TextBetween(path, '/' & not_unit_org_id & '/', '/')), parent_id)) as dep_id,
    path as div_path,
    case (not_unit_org_id = id)
      when true then 0 
      else 
        case (not_unit_org_id = parent_id)
          when true then 1
          else 2
        end
    end as div_flag, // 0 - гос. орган, 1 - департамент, 2 - отдел
    parent_id as div_parent_id,
    not_unit_org_id as div_not_unit_org_id,
    organization_form_ru as div_organization_form_ru,
    organization_form_kk as div_organization_form_kk,
    name_ru as div_name_ru,
    name_kk as div_name_kk,
    case (org_type_name_ru = 'Департамент министерства' and not_unit_org_id <> parent_id)
      when true then 'Отдел департамента'
      else org_type_name_ru
    end as div_org_type_name_ru,
    location_id as div_location_id,
    deleted as div_deleted,
    parent_chain_ru as div_parent_chain_ru ,
    parent_chain_kk as div_parent_chain_kk
  FROM transform.organizations ) AS subq


// 5. Преобразование таблицы appeals_history
// Так как нам нужно учитывать все перенаправления обращений, считать просрочки по перенаправлениям, 
// то много данных будем брать из таблицы appeals_history. 
// На этом уровне преобразуем ее, добавив значения в пустые ячейки и изменив порядок записей. 
// Для type = FORWARD не всегда указан target_org_id (к кому перенаправляют обращение). 
// Добавим их из следующей записи (type = 'CHANGE_STATUS' and state = 'IN_PROGRESS') из поля actor_org_id.
// Для type = UPWARD не всегда указан target_org_id. 
// Добавим их, подставив родительскую организацию инициатора действия parent_id(actor_org_id). 
// Таблица соответствия родительских организаций
transform.map_parent_id
SELECT
  id,
  parent_id
FROM transform.organizations;


// Таблица соответствия удаленных обращений
transform.map_appeal_deleted
SELECT
  id,
  deleted
FROM stage.appeals


// По порядку сначала должна идти запись о подписании перенаправления и после этого само перенаправление. 
// Но часто это происходит в одно время и бывает, что порядок стоит наоборот. 
// Поэтому для записей, где type = FORWARD_SIGNED идет после type = FORWARD 
// и их даты равны, добавляем для FORWARD epsilon к date, чтобы исправить порядок
// Загружаем записи, где type = 'FORWARD'  и 'FORWARD_SIGNED'
transform.tmp_forward
SELECT
  id,
  appeal_id,
  "type",
  "date"
FROM stage.appeals_history_new
WHERE type in ('FORWARD','FORWARD_SIGNED');    


// Таблица соответствия удаленных обращений
transform.map_appeal_deleted
SELECT
  id,
  deleted
FROM stage.appeals.qvd;


// По порядку сначала должна идти запись о подписании перенаправления и после этого само перенаправление. 
// Но часто это происходит в одно время и бывает, что порядок  стоит наоборот. 
// Поэтому для записей, где type = FORWARD_SIGNED идет после type = FORWARD и их даты равны, 
// добавляем для FORWARD epsilon к date, чтобы исправить порядок
transform.tmp_forward
SELECT
  id,
  appeal_id,
  type,
  date
FROM stage.appeals_history_new.qvd
where match(type, 'FORWARD','FORWARD_SIGNED');    
 
// Задаем переменную epsilon
let epsilon = 0.00000005;


// Загружаем данные, отсортировав по appeal_id и убыванию даты. 
// То есть строка с type = 'FORWARD' должна идти до записи с type = 'FORWARD_SIGNED'.
// peek() - функция подставляет значение предыдущей загруженной строки. 
// Условием проверяем если предыдущая запись имеет такой же appeal_id, текущий type = 'FORWARD'   
// и предыдущий загруженный type = 'FORWARD_SIGNED' и их даты идентичны, то к date добавляем epsilon и задаем формат даты.
// Аналогично создаем поле с флагом, где даты были идентичны.
transform.tmp_forward_new_date
SELECT
  id,
  appeal_id,
  type,
  date,
  CASE (appeal_id = prev_appeal_id 
        and type = 'FORWARD'
        and prev_type = 'FORWARD_SIGNED'
        and cast(extract(epoch from date) as integer) = cast(extract(epoch from prev_date) as integer) )
    WHEN true THEN to_timestamp(cast(extract(epoch from date) as integer) + epsilon)
    ELSE date 
  END AS new_date,         
  CASE (appeal_id = prev_appeal_id 
        and type = 'FORWARD'
        and prev_type = 'FORWARD_SIGNED'
        and cast(extract(epoch from date) as integer) = cast(extract(epoch from prev_date) as integer) )
    WHEN true THEN 1
    ELSE 0 
  END AS flag_same_dates
FROM( SELECT
    id,
    appeal_id,
    type,
    date,
    LEAD(appeal_id, -1, 0) over (partition by appeal_id order by date desc) AS prev_appeal_id,
    LEAD(type, -1, 'NA') over (partition by appeal_id order by date desc) AS prev_type,
    LEAD(type, -1, date) over (partition by appeal_id order by date desc) AS prev_date
  FROM transform.tmp_forward
  ORDER BY appeal_id, date desc) AS subq


// Создаем таблицу соответствий для новых дат из предыдущей таблицы, выбирая те записи, где flag_same_dates = 1
transform.map_new_date_forward
SELECT
  id,
  new_date
from transform.resident tmp_forward_new_date
where flag_same_dates = 1;


// Загружаем таблицу из appeals_history_new.qvd. '
// Заменяем из предыдущей таблицы соответствия новые даты 
// с учетом epsilon и помечаем записи удаленных обращений.
transform.tmp_history
SELECT
  id,
  appeal_id,
  "type",
  state,
  working_state,
  (SELECT new_date FROM transform.map_new_date_forward WHERE id = tbl.id) AS date,
  (SELECT deleted FROM transform.map_appeal_deleted WHERE id = tbl.appeal_id) as appeal_history_deleted,
  actor_org_id,
  target_org_id,
  actor_user_id,
  system_message_ru,
  system_message_kk
FROM stage.appeals_history_new;

// После записи с type = 'FORWARD'  должна идти запись с type = 'CHANGE_STATUS' и state = 'IN_PROGRESS'. 
// Загружаем из предыдущей таблицы записи с такими параметрами, отсортировав по appeal_id и убыванию даты. 
// Если для записи с type = 'CHANGE_STATUS'  значение поля target_org_id пусто, то берем значение из предыдущей загруженной записи из поля actor_org_id. 
// То есть из записи, которая идет следующей после перенаправления. И создаем поле с флагом пустого значения target_org_id.
*/
transform.еmpty_target
SELECT
  id,
  appeal_id,
  "type",
  "date",
  actor_org_id,
  CASE 
    (target_org_id IS NULL and type = 'FORWARD')
    WHEN true THEN 1
    ELSE 0
  END AS target_org_id_null,
  CASE 
    (target_org_id IS NULL and type = 'FORWARD')
    WHEN true THEN LEAD(actor_org_id, -1, 0) over (partition by appeal_id order by date desc)
    ELSE target_org_id
  END AS target_org_id
FROM transform.tmp_history
where type = 'FORWARD' 
  or (type = 'CHANGE_STATUS' and state = 'IN_PROGRESS')
order by appeal_id, date desc;



// Создаем таблицу соответствия для записей подстановки target_org_id  в записи с
// пустыми значениями.
transform.map_empty_target
SELECT
  id,
  target_org_id
FROM transfor.empty_target
WHERE 
target_org_id_null = 1;


// Собираем итоговую таблицу appeals_history. 
// Создаем поле date_previous_state с датой предыдущей записи, 
// кроме записей с созданием обращения (state = 'CREATED' and working_state = 'NEW').
// Для пустых значений target_org_id  заменяем либо из таблицы 'map_empty_target' 
// для записей с type = 'FORWARD'  либо из таблицы 'map_parent_id'  
// для записей type = 'UPWARD', либо оставляем значение target_org_id, если не оно не пусто.

transform.appeals_history
  // Дата предыдущего статуса. Нужна для расчета времени на переход между статусами
  if(IsNull(target_org_id) and type = 'FORWARD', ApplyMap('map_empty_target', id), if (IsNull(target_org_id) and type = 'UPWARD', ApplyMap('map_parent_id', actor_org_id), target_org_id)) as target_org_id,
SELECT
  id,
  appeal_id,
  "type",
  state,
  working_state,
  "date",
  actor_org_id,
  CASE (state = 'CREATED' and working_state = 'NEW')
    WHEN true THEN NULL
    ELSE LEAD(date, -1, 0) over (partition by appeal_id order by date desc)
  END as date_previous_state,
  target_ord_id_forward,
  target_ord_id_upward,
  CASE (target_org_id IS NULL and type = 'FORWARD')
    WHEN true THEN target_ord_id_forward
    ELSE 
      CASE (target_org_id IS NULL and type = 'UPWARD') 
        WHEN true THEN target_ord_id_upward
        ELSE target_org_id
      END
  END AS target_org_id,
  actor_user_id,
  system_message_ru,
  system_message_kk,
  appeal_history_deleted
FROM (
  SELECT
  id,
  appeal_id,
  "type",
  state,
  working_state,
  "date",
  actor_org_id,
  CASE (state = 'CREATED' and working_state = 'NEW')
    WHEN true THEN NULL
    ELSE LEAD(date, -1, 0) over (partition by appeal_id order by date desc)
  END as date_previous_state,
  (SELECT target_org_id FROM transform.map_empty_target WHERE id = tbl.id) AS target_ord_id_forward,
  (SELECT parent_id FROM transform.map_parent_id WHERE id = tbl.actor_org_id) AS target_ord_id_upward,
  actor_user_id,
  system_message_ru,
  system_message_kk,
  appeal_history_deleted
FROM transform.tmp_history
Order by appeal_id, date) AS subq


// 6. Создаем таблицы с полными и частичными перенаправлениями
// Таблицы будут созданы на основании таблицы History.
// Полные перенаправления определяются из записей таблицы History,
// где type = FORWARD или UPWARD. 
// Частичные перенаправления определяются из записей с type = 'CHANGE_STATUS' and state = 'CREATED'
// и где в регистрационном номере обращения встречается /. 
// Записи, где  type = PARTIAL_FORWARD будем игнорировать.
// Через appeal_id подставляем регистрационный номер обращения. 
// Для того, чтобы определить номер родительского обращения (из какого оно было перенаправлено в части), 
// нужно убрать из регистрационного номера справа ‘/x’. 
// Например, для обращения с рег. номером 22222/1/3 родительским обращением будет обращение с рег. номером 22222/1. 
// Далее, подставив через таблицу соответствия подставляем appeal_id, получим id родительского обращения. 
// Таким образом из одной записи мы получим кто перенаправил в части (actor_org_id) и из какого обращения (id родительского) 
// и куда (target_org_id) и в какое обращение (appeal_id), 
transform.regnum_appeals
SELECT
  id,
  reg_number
FROM stage.appeals.qvd;

// Таблица соответствия id обращения и его рег. номера
transform.map_appeals_reg_number
SELECT
    id,
    reg_number
FROM transform.regnum_appeals;

// Таблица соответствия рег. номера и его id обращения
transform.map_appeal_id
SELECT
  reg_number,
  id
FROM transform.regnum_appeals;


// Таблица соответствия id организации и ее пути
transform.map_path
SELECT
  id,
  path
FROM transform.organizations;


// Таблица соответствия id организации и ее ближайшего гос. органа. Если организация
// является гос. органом, то значения будут равны.
transform.map_not_unit_org_id
SELECT
  id,
  not_unit_org_id
FROM transform.organizations;


// Таблица соответствия id организации и кода группы организации
transform.map_group_code
SELECT
  id,
  group_code
FROM transform.organizations
WHERE not_unit_org_id = id;

// Таблица соответствия кода и названия групп организации
transform.map_group_name_ru
SELECT
  group_code,
  group_name_ru
FROM transform.organizations
where not_unit_org_id = id;

transform.map_group_name_kk
SELECT
  group_code,
  group_name_kk
FROM transform.organizations
where not_unit_org_id = id;


// Ниже код нужно смотреть с 4го LOAD. 
// Третий LOAD сразу преобразует таблицу из нижнего уровня. Второй LOAD из третьего и первый из второго. 
// 4ый уровень. Загружаем из таблицы History строки с полным перенаправлением (type = 'FORWARD', 'UPWARD'  
// и target_org_id <> actor_org_id, чтобы исключить случаи, когда перенаправими в саму себя) 
// и записью о создании обращения (type = 'CHANGE_STATUS' and state = 'CREATED').
// На этом этапе создаем поля даты переадресации (прибавляем к date часовой пояс инициатора перенаправления actor_org_id), 
// даты регистрации (прибавляем к date часовой пояс организации, куда перенаправили target_org_id), 
// через appeal_id подставляем рег. номер и подставляем ближайший гос. орган для actor_org_id и target_org_id.


transform.history_forward
// 4ый уровень. Сначала загружается эта таблица.
SELECT
  id,
  appeal_id,
  -- transform.map_appeals_reg_number: id, reg_number
  (SELECT reg_number FROM transform.map_appeals_reg_number WHERE id = tbl.appeal_id) as reg_number,
  "type",
  state,
  working_state,
  -- transform.map_org_utc: id::text, value::text
  "date" 
    + 
  (SELECT COALESCE(TO_TIMESTAMP(value, 'YYYY-MM-DD HH:MI:SS'),0) FROM transform.map_org_utc WHERE id = tbl.actor_org_id)/24 
  as redirect_date, // Дата полного перенаправления с учетом часового пояса инициатора действия
  "date" 
    + 
  (SELECT COALESCE(TO_TIMESTAMP(value, 'YYYY-MM-DD HH:MI:SS'),0) FROM transform.map_org_utc WHERE id = tbl.target_org_id::text)/24 
  as registration_date, // Дата регистрации обращения с учетом часового времени куда поступило 
  -- transform.map_not_unit_org_id : id, not_unit_org_id
  (SELECT value FROM transform.map_not_unit_org_id WHERE id = tbl.actor_org_id) AS actor_not_unit_org_id, // Ближайший гос. орган инициатора действия
  (SELECT value FROM transform.map_not_unit_org_id WHERE id = tbl.target_org_id) AS target_not_unit_org_id, // Ближайший гос. орган, на кого направлено действие.
  actor_org_id,
  target_org_id,
  actor_user_id,
  system_message_ru,
  system_message_kk
FROM stage.history AS tbl
where (type in ('FORWARD', 'UPWARD') and target_org_id <> actor_org_id) 
         or 
      (type = 'CHANGE_STATUS' and state = 'CREATED');    

// 3ий уровень
// LEFT JOIN (and adding all columns from below to history_forward by ID)
SELECT
  id,
  -- transform.map_group_code: id, group_code
  (SELECT group_code FROM transform.map_group_code WHERE id = tbl.actor_not_unit_org_id) AS actor_unit_code,
  -- transform.map_parent_id: id, parent_id
  (SELECT parent_id FROM transform.map_parent_id WHERE id = tbl.actor_not_unit_org_id) AS actor_unit_parent_id, 
  -- transform.map_path: id, path
  (SELECT path FROM transform.map_path WHERE id = tbl.actor_not_unit_org_id) AS actor_unit_path,
  -- transform.map_group_code: id, group_code
  (SELECT group_code FROM transform.map_group_code WHERE id = tbl.target_not_unit_org_id) as target_unit_code,
  -- transform.map_parent_id: id, parent_id
  (SELECT parent_id FROM transform.map_parent_id WHERE id = tbl.target_not_unit_org_id) as target_unit_parent_id,
  -- transform.map_path: id, path
  (SELECT path FROM transform.map_path WHERE id = tbl.target_not_unit_org_id) as target_unit_path,
  CASE (date_part('isodow', registration_date) BETWEEN 1 AND 5) 
        AND registration_date NOT IN ($holidays)
        AND (registration_date::time < make_time(18,30, 0) )
    WHEN true THEN registration_date
    ELSE
      CASE (date_part('isodow', registration_date) BETWEEN 5 AND 6) OR (registration_date IN $holidays)
        WHEN true THEN ( SELECT
                           MIN($registration_date + subq.generate_series) 
                         FROM
                           (select generate_series from generate_series(1,7) ) as subq
                         WHERE subq.generate_series not in $holidays
                           AND date_part('isodow', subq.generate_series) not in (6,7) )
        ELSE
          CASE (registration_date::time < make_time(18,30, 0) )
            WHEN true THEN 
                        CASE (date_part('isodow', registration_date+1) BETWEEN 1 AND 5)
                          WHEN true THEN registration_date+1
                          ELSE
                            ( SELECT
                                MIN($registration_date + subq.generate_series) 
                              FROM
                                (select generate_series from generate_series(1,7) ) as subq
                              WHERE subq.generate_series not in $holidays
                                 AND date_part('isodow', subq.generate_series) not in (6,7) )
            ELSE registration_date
          END
      END as date_start_work // Принято в работу. Дата
FROM stage.history AS tbl
where (type in ('FORWARD', 'UPWARD') and target_org_id <> actor_org_id) 
         or 
      (type = 'CHANGE_STATUS' and state = 'CREATED');

// 2ой уровень
// LEFT JOIN (and adding all columns from below to history_forward by ID)
SELECT
  id,
  CASE (actor_unit_code IN (6,9) AND array_length(string_to_array(actor_unit_path, target_unit_path), 1) - 1 = 0)
    WHEN true THEN 10
    ELSE actor_unit_code
  END AS redirect_from_code,
  CASE (target_unit_code IN (6,9) AND array_length(string_to_array(target_unit_path, actor_unit_path), 1) - 1 = 0)
    WHEN true THEN 10
    ELSE target_unit_code
  END AS redirect_to_code,
  CASE (type = 'UPWARD' OR (type = 'FORWARD' and target_not_unit_org_id = actor_unit_parent_id) OR (type = 'CHANGE_STATUS' and target_not_unit_org_id = actor_unit_parent_id) )
    WHEN true THEN 'Да'
    ELSE 'Нет'
  END AS flag_redirect_to_higher_go_ru,
  CASE (array_length(string_to_array(target_unit_path, '/' || actor_not_unit_org_id || '/'), 1) - 1 > 0
        OR 
        array_length(string_to_array(actor_unit_path, '/' || target_not_unit_org_id || '/'), 1) - 1 > 0)
    WHEN true THEN 'Вертикальная'
    ELSE 'Горизонтальная'
   END AS redirect_type_ru
FROM stage.history AS tbl
where (type in ('FORWARD', 'UPWARD') and target_org_id <> actor_org_id) 
         or 
      (type = 'CHANGE_STATUS' and state = 'CREATED');

// 1ый уровень
// LEFT JOIN (and adding all columns from below to history_forward by ID)
SELECT
  id,
  -- transform.map_group_name_ru: group_code, group_name_ru
  (SELECT group_name_ru FROM transform.map_group_name_ru WHERE group_code = tbl.redirect_from_code) AS redirect_from_text_ru, // Перенаправлено из
  -- transform.map_group_name_kk: group_code, group_name_kk
  (SELECT group_name_kk FROM transform.map_group_name_kk WHERE group_code = tbl.redirect_from_code) AS redirect_from_text_kk,
  -- transform.map_group_name_ru: group_code, group_name_ru
  (SELECT group_name_ru FROM transform.map_group_name_ru WHERE group_code = tbl.redirect_to_code) AS redirect_to_text_ru, // Перенаправлено в
  -- transform.map_group_name_kk: group_code, group_name_kk
  (SELECT group_name_kk FROM transform.map_group_name_kk WHERE group_code = tbl.redirect_to_code) AS redirect_to_text_kk,
  CASE flag_redirect_to_higher_go_ru = 'Да'
    WHEN true THEN 'Иә'
    ELSE 'Жоқ'
  END AS flag_redirect_to_higher_go_kk,
  CASE redirect_type_ru = 'Вертикальная'
    WHEN true THEN 'Тігінен'
    ELSE 'Көлденең' 
   END as redirect_type_kk
FROM stage.history AS tbl
where (type in ('FORWARD', 'UPWARD') and target_org_id <> actor_org_id) 
         or 
      (type = 'CHANGE_STATUS' and state = 'CREATED');

// Создадим таблицу для полных перенаправлений
// Сначала из таблицы History_forward загрузим только записи с type, 'FORWARD', 
// 'UPWARD' и target_org_id <> actor_org_id
transform.full_redirects
SELECT
  id,
  appeal_id,
  "type",
  state,
  working_state,
  redirect_date,
  date_start_work,
  registration_date,
  actor_not_unit_org_id,
  target_not_unit_org_id,
  actor_org_id,
  target_org_id,
  actor_user_id,
  system_message_ru,
  system_message_kk,    
  1 as was_redirected,
  'Полное' as flag_redirect_ru,
  'Толық' as flag_redirect_kk,
  redirect_from_code,
  redirect_to_code,
  redirect_from_text_ru, // Перенаправлено из
  redirect_from_text_kk, 
  redirect_to_text_ru, // Перенаправлено в
  redirect_to_text_kk,
  flag_redirect_to_higher_go_kk,
  redirect_type_kk,
  flag_redirect_to_higher_go_ru, // Передано в вышестоящий ГО
  redirect_type_ru
from transform.history_forward
where type in ('FORWARD', 'UPWARD') 
  and target_org_id <> actor_org_id;


// Добавляем дату предыдущей регистрации обращения и предыдущею дату принятия в работу, 
// чтобы определить просрочку по переадресации
transform.previous_date
SELECT
  id,
  type,
  redirect_date,
  date_start_work,
  registration_date,
  CASE type <> 'CHANGE_STATUS' 
    WHEN true THEN LEAD(date_start_work, -1) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END as previous_date_start_work, // Предыдущая дата принятия в работу 
  CASE type <> 'CHANGE_STATUS' 
    WHEN true THEN LEAD(registration_date, -1) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END AS previous_registration_date // Предыдущая дата регистрации
from transform.history_forward
order by appeal_id, redirect_date asc;

// Выбираем только записи с type = 'FORWARD', 'UPWARD' из предыдущей таблицы
// Добавляем поле с количеством дней на перенаправление и присоединяем к таблице Full_redirects.
// Функция NetWorkDays считает количество рабочих дней между двумя датами 
// с учетом выходных и праздников в переменной holidays. Тут не учтены рабочие субботние дни.
// LEFT JOIN (and adding all columns from below to "full_redirects" by ID)
SELECT
  id,
  previous_date_start_work, // Предыдущая дата принятия в работу 
  previous_registration_date, // Предыдущая дата регистрации
  CASE previous_date_start_work::date = redirect_date::date
    WHEN true THEN 1
    ELSE ( SELECT
             COUNT(subq.dd)
           FROM ( SELECT date_trunc('day', dd)::date AS dd
                  FROM generate_series(tbl.previous_date_start_work::timestamp, tbl.redirect_date::date::timestamp, '1 day'::interval) dd ) AS subq
           WHERE date_part('isodow', subq.dd) NOT IN (6,7)
             AND subq.dd > tbl.previous_date_start_work
             AND subq.dd < tbl.redirect_date
             AND subq.dd NOT IN ($holidays)
        )
  END AS count_days_holidays
FROM transform.previous_date
WHERE type IN ('FORWARD', 'UPWARD');

// Добавляем откуда было прошлое перенапрваление (организация, департамент(отдел), код группы организации 
// и группа организации(например, из АП или КПМ), была ли переадресация вертикальная 
// или горизонтальная и была ли в вышестоящий ГО.
transform.previous_data
SELECT
  id,
  appeal_id,
  type,
  redirect_date,
  actor_org_id,
  actor_not_unit_org_id,
  actor_user_id,
  redirect_from_code,
  flag_redirect_to_higher_go_ru,
  CASE appeal_id = LEAD(appeal_id, -1) over (partition by appeal_id order by redirect_date)
    WHEN true THEN LEAD(actor_org_id, -1) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END as previous_actor_org_id, // От кого была прошлая переадресация (организация или отдел)
  CASE appeal_id = LEAD(appeal_id, -1) over (partition by appeal_id order by redirect_date)
    WHEN true THEN LEAD(actor_not_unit_org_id, -1) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END as previous_actor_not_unit_org_id, // От кого была прошлая переадресация (организация)
  CASE appeal_id = LEAD(appeal_id, -1) over (partition by appeal_id order by redirect_date)
    WHEN true THEN LEAD(redirect_from_code, -1, NULL) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END as previous_redirect_from_code,  // От кого была прошлая переадресация (группа организации) 
  CASE appeal_id = LEAD(appeal_id, -1) over (partition by appeal_id order by redirect_date)
    WHEN true THEN LEAD(actor_user_id, -1, NULL) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END as previous_actor_user_id,  // От кого была прошлая переадресация (id инициатора)
  CASE appeal_id = LEAD(appeal_id, -1) over (partition by appeal_id order by redirect_date)
    WHEN true THEN LEAD(flag_redirect_to_higher_go_ru, -1, NULL) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END as previous_flag_redirect_to_higher_go_ru,  // Была ли прошлая переадресация в вышестоящий ГО
  CASE appeal_id = LEAD(appeal_id, -1) over (partition by appeal_id order by redirect_date)
    WHEN true THEN LEAD(redirect_type_ru, -1, NULL) over (partition by appeal_id order by redirect_date)
    ELSE NULL
  END as previous_redirect_type_ru  // Прошлая переадресация по вертикали или горизонтали   
FROM transform.history_forward
WHERE (type IN ('FORWARD', 'UPWARD') and target_org_id <> actor_org_id) or 
      ( (type = 'CHANGE_STATUS' and state = 'CREATED') and array_length(string_to_array(reg_number, '/'), 1) - 1 > 0) 
order by appeal_id, redirect_date asc;


transform.full_redirects
SELECT
  id,
  previous_actor_org_id,
  previous_actor_not_unit_org_id,
  previous_redirect_from_code,
  previous_actor_user_id,
  previous_flag_redirect_to_higher_go_ru
FROM transform.previous_data
WHERE type IN ('FORWARD', 'UPWARD');

// Добавим к таблице Full_redirects дату первого полного перенаправления. 
// На этом этапе таблица Full_redirects еще понадобится на этапе преобразования таблицы appeals.
transform.full_redirects
SELECT
  appeal_id,
  min(redirect_date)::date as first_redirect_date
FROM transform.full_redirects
where type IN ('FORWARD', 'UPWARD')
group by appeal_id;


// Создадим таблицу для частичных перенаправлений.
// Сначала загружаем уровень 3, затем уровень 2 и далее 1. 
// На 2 уровне просто загружаем данные с частичными перенаправлениями из таблицы History_forward, 
// устанавливаем флаг Частичное для всех записей таблицы и переименовываем поле с рег. номером.
// На 1 уровне из рег. номера обращения перенаправленного в части получаем рег. номер и id родительского обращения.
transform.part_redirects
LOAD // 1ый уровень
	*,
// 2ой уровень
SELECT
  id,
  appeal_id,
  "type",
  state,
  working_state,
  redirect_date,
  date_start_work,
  registration_date,
  actor_not_unit_org_id,
  target_not_unit_org_id,
  actor_org_id,
  target_org_id,
  actor_user_id,
  system_message_ru,
  system_message_kk,
  2 as was_redirected,
  'Частичное' as flag_redirect_ru,
  'Ішінара' as flag_redirect_kk,    
  redirect_from_code,
  redirect_to_code,
  redirect_from_text_ru, // Перенаправлено из
  redirect_from_text_kk, 
  redirect_to_text_ru, // Перенаправлено в
  redirect_to_text_kk,
  flag_redirect_to_higher_go_kk,
  redirect_type_kk,
  flag_redirect_to_higher_go_ru, // Частичное перенапраление в вышестоящий ГО
  redirect_type_ru,
  reg_number as reg_number_part_redir // В какое обращение перенаправили частично
  LEFT(reg_number_part_redir, length(reg_number_part_redir) - 1 - length(( select arr[array_upper(arr, 1)] FROM       
                                                                          (SELECT string_to_array(reg_number_part_redir, '/') as arr) as subq
                                                                        )))) as parent_appeal_regnum,
  -- transform.map_appeal_id: reg_number, id
  (SELECT reg_number transform.map_appeal_id WHERE id = left(tbl.reg_number_part_redir, length(tbl.reg_number_part_redir) - 1 -len(( select arr[array_upper(arr, 1)] FROM       
                                                                                                                                     (SELECT string_to_array(reg_number_part_redir, '/') as arr) as subq
                                                                                                                                   )))) as parent_appeal_id
FROM transform.history_forward AS tbl
WHERE type = 'CHANGE_STATUS' 
  AND state = 'CREATED' 
  and actor_org_id IS NOT NULL
  and target_org_id IS NOT NULL
  and array_length(string_to_array(reg_number, '/'), 1) - 1 > 0;

// LEFT JOIN (and adding all columns from below to "full_redirects" by ID)
SELECT
  id,
  previous_actor_org_id,
  previous_actor_not_unit_org_id,
  previous_redirect_from_code,
  previous_actor_user_id,
  previous_flag_redirect_to_higher_go_ru
FROM transform.previous_data
where type IN ('FORWARD', 'UPWARD');


// 7. Собираем таблицу со справочником Категория - Подкатегория - Характер вопроса
// В таблице с характером вопроса есть ссылка на подкатегорию, 
// в таблице с подкатегориями есть ссылка на категорию. 
// Поэтому прикрепляем последовательно к таблице с ХВ.
// Далее эту таблицу прикрепим к таблице с обращениями.
// Характер обращения
transform.issues_subcategories:
SELECT
  id as subissue_category_id,
  category_id,
  Upper(trim(name_ru)) as tmp_subissue_category_ru,
  Upper(trim(name_kk)) as tmp_subissue_category_kk,
  CASE (actual = 'False')
    WHEN true THEN 'Неактуальные значения'
    ELSE 'Актуальные значения'
  END as subissue_category_actual
FROM transform.issues_subcategories;


//Подкатегория
// LEFT JOIN (and adding all columns from below to "issues_subcategories" by ID)
SELECT
  id as category_id,
  Upper(trim(name_ru)) as tmp_issue_category_ru,
  Upper(trim(name_kk)) as tmp_issue_category_kk,
  section_id
FROM transform.issues_categories;

// Категория
// LEFT JOIN (and adding all columns from below to "issues_sections" by ID)
SELECT
  id as section_id,
  Upper(trim(name_ru)) as tmp_issue_section_name_ru,
  Upper(trim(name_kk)) as tmp_issue_section_name_kk
FROM transform.issues_sections;



// 8. Собираем таблицу с обращениями 
// Основная таблица с обращениями. 
// Датой регистрации обращения для исполнителя будет либо дата регистрации, если не было перенаправлений, 
// либо дата регистрации после последнего перенаправления. 
// Поэтому надо определить последнюю дату регистрации (перенаправления). 
// А также определим количество перенаправлений в обращении и цепочку перенаправлений(когда - от кого - куда).
// Если обращение было полностью переадресовано, то дата регистрации и дата принятия в работу должна быть датой последней операции по перенаправлению.
// Из таблицы Full_redirects загружаем данные, группируя по appeal_id. Создаем новые поля на основании данных таблицы

transform.full_forward_group
SELECT
  appeal_id,
  MAX(registration_date) OVER (PARTITION BY appeal_id) AS last_registration_date, // последняя дата регистрации
  MAX(date_start_work) OVER (PARTITION BY appeal_id) AS last_date_start_work, // последняя дата принято в работу
  STRING_AGG(registration_date || ': ' || CHR(10132) || ' ' || (SELECT name_ru FROM transform.map_organization_names_ru WHERE id = tbl.target_org_id) || chr(10) || redirect_date, '') AS forward_chain_ru,
  STRING_AGG(registration_date || ': ' || CHR(10132) || ' ' || (SELECT name_kk FROM transform.map_organization_names_kk WHERE id = tbl.target_org_id) || chr(10) || redirect_date, '') AS forward_chain_kk
from transform.full_redirects AS tbl
group by appeal_id;

transform.map_last_registration_date
SELECT
  appeal_id,
  last_registration_date
FROM transform.full_forward_group;

// Таблица соответствия с последней датой принятия в работу
transform.map_last_date_start_work
SELECT
  appeal_id,
  last_date_start_work
FROM transform.full_forward_group;


// Рабочие статусы в таблице appeals не отображают текущие статусы обращений, как в системе в карточке обращения в истории статусов. 
// Они находятся в appeals_history. Поэтому берем последнее сочетание полей type и working_state из appeals_history и подставляем его в appeals. 
// В случае, когда запись для обращения в appeals_history еще не появилась, будем брать альтернативный статус через ключ current_state-current_working_state и таблицы соответствия 'map_appeal_alternative_working_status_ru' и 'map_appeal_alternative_working_status_ru'.
// Общий рабочий статус - это в работе, зарегистрировано, исполнено и еще несколько
// Рабочий статус - это более детальное, как в истории статусов в карточке обращений. Например, отправлено на доработку.
transform.map_appeal_key_last_working_status
SELECT
  appeal_id,
  FIRST_VALUE(type || '-' || working_state) OVER (PARTITION BY appeal_id ORDER BY date DESC) AS last_ws
FROM transform.history
group by appeal_id;


// Соисполнитель. Если executor_id <> user_id, то user_id - соисполнитель.
transform.map_executors_appeal
SELECT
  appeal_id,
  user_id
FROM stage.appeal_exeсutors;

// Иногда, обычно для новых обращений не успевает появиться запись в appeals_history, 
// поэтому статус из appeals_history не подойдет. В 
// этом случае будем использовать внутренний и публичный статус
// KeyAlternativeStatus = current_state-current_working_state из таблицы appeals
transform.map_appeal_alternative_working_status_ru
SELECT
  KeyAlternativeStatus,
  Upper(trim("Рабочий статус")) as "Рабочий статус"
FROM transform.mapping_working_states;

// Рабочие статусы как в системе в карточке обращения. 
// Образуются на основании комбинации полей type и working_state. 
// Значения общих и рабочих статусов загружаю из Excel файла. Он будет в дополнении к этому документу. 
// KeyWorkingState = type-working_state из таблицы appeals_history
transform.map_appeal_working_status_ru
SELECT
  KeyWorkingState,
  Upper(trim("Рабочий статус")) as "Рабочий статус"
FROM transform.Mapping_history_states.qvd;


// Общие рабочие статусы: Зарегистрировано, В работе, Исполнено. 
// Будут использоваться для подсчета общего количества обращений в этих статусах
transform.map_appeal_general_working_status_ru
SELECT
  KeyWorkingState,
  Upper(trim("Общий статус обращения")) as "Общий статус обращения"
FROM transform.Mapping_history_states;

transform.tmp_appeals
SELECT
  appeal_id, // Номер талона
  reg_number, // Регистрационный номер
  UPPER( CASE(current_working_state = 'FINISHED')
           WHEN true THEN 'Исполнено'
           ELSE (SELECT COALESCE("Общий статус обращения", q.map_appeal_alternative_working_status_ru_val) FROM transform.Mapping_history_states WHERE KeyWorkingState = q.map_appeal_key_last_working_status_val)
         END
        ) as current_general_working_state_ru, // Общий статус
  -- transform.map_appeal_working_status_ru: KeyWorkingState, "Рабочий статус"
  (SELECT COALESCE("Рабочий статус", q.map_appeal_key_last_working_status_val) FROM transform.map_appeal_working_status_ru WHERE KeyWorkingState = q.map_appeal_alternative_working_status_ru_val)
  on_signing_flag,
  created_date,  // Дата регистрации обращения без учета часового пояса.  
  start_date,  // Дата начала работы по обращению с учетом подачи обращения после 18:30    
  deadline, // срок рассмотрения 
  finish_date,   // дата ответа
  flag_answer_appeal,
  executor_org_id, // Организация, где сейчас находится обращение. Исполнитель/Регистратор 
  CASE executor_org_id <> org_id
    WHEN true THEN  org_id
    ELSE NULL
  END as executor_dep_id, // Департамент организации, где сейчас находится обращение.
  executor_timezone, // Часовой пояс организации, у кого находится обращение
  signer, // Кто подписал ЭЦПшкой итоговое решение об исполнении обращения
  appeal_source_ru, // Канал поступления обращения
  appeal_source_kk,
  appeal_source_id, // Канал поступления обращения 
  hidden as appeal_hidden, // Скрытое обращение тру фолс
  executive_org_id, // Ближайший гос. орган исполнительной организации (если исполнял департамент или отдел)
  CASE executive_org_id_val <> executive_org_id
    WHEN true THEN executive_org_id
    ELSE NULL
  END as executive_dev_id, // Непосредственно исполнительная организация (может быть структурное подразделение, отдел и возможно не дочерняя организация)
  prolong_count,
  complaint_exist, // Признак что обращение отправлялось на дороботку из-за несответствия статьи 63 и 93
  executor_id, // Основной исполнитель
  CASE (co_executor_id_val <> executor_id)
    WHEN true THEN co_executor_id_val
    ELSE NULL 
  END as co_executor_id, // Соисполнитель
  URL, 
  is_duplicate_of,
  flag_duplicate,
  deleted as appeal_deleted, // флаг удаленного обращения
  prolong_days
FROM (
SELECT
  appeal_id, // Номер талона
  reg_number, // Регистрационный номер
  -- transform.map_appeal_key_last_working_status: appeal_id, last_ws
  (SELECT last_ws FROM transform.map_appeal_key_last_working_status WHERE id = tbl.id) AS map_appeal_key_last_working_status_val,
  -- transform.map_appeal_alternative_working_status_ru: KeyAlternativeStatus, "Рабочий статус"
  (SELECT "Рабочий статус" FROM transform.map_appeal_alternative_working_status_ru WHERE KeyAlternativeStatus = current_state || '-' || current_working_state) AS map_appeal_alternative_working_status_ru_val,
  -- transform.map_appeal_working_status_ru: KeyWorkingState, "Рабочий статус"
  (SELECT "Рабочий статус" FROM transform.map_appeal_working_status_ru WHERE KeyWorkingState = current_state || '-' || current_working_state) AS map_appeal_alternative_working_status_ru_val,
  -- transform.map_appeal_key_last_working_status: appeal_id, last_ws
  (SELECT last_ws FROM transform.map_appeal_key_last_working_status WHERE appeal_id = tbl.id) AS map_appeal_key_last_working_status_val,
  -- transform.map_appeal_alternative_working_status_ru: KeyAlternativeStatus, "Рабочий статус"
  (SELECT "Рабочий статус" FROM transform.map_appeal_alternative_working_status_ru WHERE KeyAlternativeStatus = current_state || '-' || current_working_state) AS map_appeal_alternative_working_status_ru_val,
  CASE (left(current_working_state, 10) ='ON_SIGNING')
     WHEN true then 1
     ELSE 0
  END AS on_signing_flag,
  created_date,  // Дата регистрации обращения без учета часового пояса.  
  start_date,  // Дата начала работы по обращению с учетом подачи обращения после 18:30    
  deadline, // срок рассмотрения 
  finish_date,   // дата ответа
  CASE (finish_date IS NULL)
    WHEN true THEN 0
    ELSE 1
  END AS flag_answer_appeal,
  -- transform.map_not_unit_org_id: id, not_unit_org_id
  (SELECT not_unit_org_id FROM transform.map_not_unit_org_id WHERE id = tbl.org_id) AS executor_org_id, // Организация, где сейчас находится обращение. Исполнитель/Регистратор 
  -- transform.map_org_utc: id, value
  (SELECT value FROM transform.map_org_utc WHERE id = tbl.org_id::text) AS executor_timezone, // Часовой пояс организации, у кого находится обращение
  signer, // Кто подписал ЭЦПшкой итоговое решение об исполнении обращения
  -- transform.map_appeal_source_ru: id, name_ru
  (SELECT COALESCE(name_ru, tbl.appeal_source_id) FROM transform.map_appeal_source_ru WHERE id = tbl.appeal_source_id) AS appeal_source_ru, // Канал поступления обращения
  (SELECT COALESCE(name_kk, tbl.appeal_source_id) FROM transform.map_appeal_source_kk WHERE id = tbl.appeal_source_id) AS appeal_source_kk, 
  appeal_source_id, // Канал поступления обращения 
  hidden as appeal_hidden, // Скрытое обращение тру фолс
  -- transform.map_not_unit_org_id: id, not_unit_org_id
  (SELECT not_unit_org_id FROM transform.map_not_unit_org_id WHERE id = tbl.executive_org_id) AS executive_org_id_val, // Ближайший гос. орган исполнительной организации (если исполнял департамент или отдел)
  prolong_count,
  complaint_exist, // Признак что обращение отправлялось на дороботку из-за несответствия статьи 63 и 93
  executor_id, // Основной исполнитель
  -- transform.map_executors_appeal: appeal_id, user_id
  (SELECT user_id FROM transform.map_executors_appeal WHERE appeal_id = tbl.id) AS co_executor_id_val, // Соисполнитель
  sid as URL, 
  is_duplicate_of,
  CASE (is_duplicate_of IS NULL)
    WHEN true THEN 0
    ELSE 1
  END AS flag_duplicate,
  deleted as appeal_deleted, // флаг удаленного обращения
  prolong_days
FROM stage.appeals AS tbl) AS q;
