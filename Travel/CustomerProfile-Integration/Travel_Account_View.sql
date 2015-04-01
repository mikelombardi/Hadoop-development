 drop VIEW if exists travel_accounts;
CREATE VIEW travel_accounts AS 
 Select
 accounts.email,
 accounts.firstname,
 accounts.surname,
 accounts.emailoptin,
 accounts.lasttouchdate
 from
 (select 
		lower(trim(p.emailaddress)) as email,
		p.firstname as firstname,
		p.surname as surname,
		lower(trim(cp.marketingoptin))  as emailoptin,
		ROW_NUMBER() OVER (PARTITION BY lower(trim(p.emailaddress))  ORDER BY rh.created desc) as RowNum,
		from_unixtime(unix_timestamp(rh.created), 'yyyyMMdd') as lasttouchdate
	from ctm_travel.policydetail p
	inner join ctm_travel.riskheader rh
		on p.aggregateid = rh.aggregateid
	left outer join ctm_travel.visit v
		on v.aggregateid = rh.journeyid
	left outer join ctm_travel.contactpreference cp
		on cp.aggregateid = rh.aggregateid
	Where v.affcliecode not like 'TST%'
) accounts
Where accounts.RowNum = 1; -- Most recent entry