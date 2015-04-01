drop VIEW if exists travelenquiry;
CREATE VIEW travelenquiry AS 


Select * from
(
Select 
    p.aggregateid uniqueid,
	ROW_NUMBER() OVER (PARTITION BY lower(trim(p.emailaddress))  ORDER BY rh.created desc) rownum,
	lower(trim(p.emailaddress)) email, 
	p.aggregateid as riskheaderid,
	cast(null as string) customersid, 
	rh.created enquirydatetime,
	p.startdate commencementdate, 
	cast(null as string) renewaldate, 
	cast(null as string) productsid, 
	'Tr' productcode, 
	'Travel' productname, 
	cast(null as string) insightsid, 
	cast(null as string) depthnormal, 
	cast(null as string) depthsimplified, 
	cast(null as string) recency, 
	cast(null as string) employmentstatussid, 
	cast(null as string) empstatuscode, 
	cast(null as string) empstatusname, 
	cast(null as string) primaryoccupationsid, 
	cast(null as string) occupationcode, 
	cast(null as string) occupationname, 
	cast(null as string)  ownershipstatuscode, 
	cast(null as string) ownershipstatusname, 
	cast(null as string) abicode, 
	cast(null as string) fulldescription, 
	cast(null as string) manufacturer,
	cast(null as string) model, 
	cast(null as string) trim, 
	cast(null as string) fueltype, 
	counts.totalclickthroughs, 
	counts.totalshowdetails,
	from_unixtime(unix_timestamp(rh.created), 'yyyyMMdd')  createdonint, 
	cast(null as string) useofothervehicle, 
	cast(null as string) vehiclesinhousehold,
	cast(null as string) additionaldrivers, 
    cast(null as string) haspets,
    from_unixtime(unix_timestamp(rh.created), 'yyyyMMdd')  lasttouchdate
from ctm_travel.policydetail p
inner join ctm_travel.riskheader rh
	on p.aggregateid = rh.aggregateid
left outer join ctm_travel.visit v
	on v.aggregateid = rh.journeyid
left outer join
		(select 
			aggregateid
			, MAX(case when action = 'ClickThrough' then 1 else 0 end) as totalclickthroughs
			, MAX(case when action = 'BridgingPanel' then 1 else 0 end) as totalshowdetails
		 from ctm_travel.visitactivity
		 Where action IN ('ClickThrough', 'BridgingPanel')
		 group by aggregateid) as counts
	on counts.aggregateid = rh.journeyid
Where v.affcliecode not like 'TST%'
) Enquiries
