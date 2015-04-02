use customer_profile;

--drop temporary tables used for processing

drop table if exists customer_profile.CustDataSet;
drop table if exists customer_profile.CustDataSet_WithSales;
drop table if exists customer_profile.CustDataSet_WithClaims;
drop table if exists customer_profile.CustDataSet_Rewards_PreProb;
drop table if exists customer_profile.CustDataSet_Rewards;

--last 11 months of cds enquiries
create table customer_profile.CustDataSet as 
Select 
lower(trim(allenquiries_history.email)) email,
allenquiries_history.riskheaderid, 
allenquiries_history.productcode, 
allenquiries_history.productname, 
allenquiries_history.enquirydatetime,
allenquiries_history.commencementdate,
allenquiries_history.totalclickthroughs, 
allenquiries_history.totalshowdetails,
allenquiries_history.abicode,
to_date(allenquiries_history.commencementdate) as GroupingDate,
concat_ws(
            '|',
coalesce(lower(trim(allenquiries_history.email)), '#'),
coalesce(to_date(allenquiries_history.commencementdate), '#'), 
coalesce(upper(trim(allenquiries_history.productcode)), '#'),
coalesce(upper(trim(allenquiries_history.abicode)), '#')
) as grouping,
    concat_ws(
            '|',
'email',
'commencementdate', 
'productcode',
'abicode'
) as grouping_desc
from cds.cds_enquiry allenquiries_history
where allenquiries_history.lasttouchdate >= cast(regexp_replace(date_add(from_unixtime(unix_timestamp()),-365),'-','') as int)
and trim(email) <> '' and email is not null
 ;
 
--append last 11 months of money enquiries
insert into table customer_profile.CustDataSet 
Select 
lower(trim(allenquiries_history.email)) email,
allenquiries_history.riskheaderid, 
allenquiries_history.productcode, 
allenquiries_history.productname, 
allenquiries_history.enquirydatetime,
allenquiries_history.commencementdate,
allenquiries_history.totalclickthroughs, 
allenquiries_history.totalshowdetails,
allenquiries_history.abicode,
to_date(allenquiries_history.commencementdate) as GroupingDate,
concat_ws(
            '|',
coalesce(lower(trim(allenquiries_history.email)), '#'),
coalesce(to_date(allenquiries_history.commencementdate), '#'), 
coalesce(upper(trim(allenquiries_history.productcode)), '#'),
coalesce(upper(trim(allenquiries_history.abicode)), '#')
) as grouping,
    concat_ws(
            '|',
'email',
'commencementdate', 
'productcode',
'abicode'
) as grouping_desc
from redpoint.moneyenquiry allenquiries_history
where to_date(allenquiries_history.enquirydatetime) >= date_add(from_unixtime(unix_timestamp()),-335)
 ;

 --append last 11 months of speedtrap enquiries - digital/landlord
 insert into table customer_profile.CustDataSet 
Select 
lower(trim(allenquiries_history.email)) email,
allenquiries_history.riskheaderid, 
allenquiries_history.productcode, 
allenquiries_history.productname, 
allenquiries_history.enquirydatetime,
allenquiries_history.commencementdate,
allenquiries_history.totalclickthroughs, 
allenquiries_history.totalshowdetails,
allenquiries_history.abicode,
to_date(allenquiries_history.commencementdate) as GroupingDate,
concat_ws(
            '|',
coalesce(lower(trim(allenquiries_history.email)), '#'),
coalesce(to_date(allenquiries_history.commencementdate), '#'), 
coalesce(upper(trim(allenquiries_history.productcode)), '#'),
coalesce(upper(trim(allenquiries_history.abicode)), '#')
) as grouping,
    concat_ws(
            '|',
'email',
'commencementdate', 
'productcode',
'abicode'
) as grouping_desc
from redpoint.speedtrapenquiry  allenquiries_history
where to_date(allenquiries_history.enquirydatetime) >= date_add(from_unixtime(unix_timestamp()),-335)
 ;

  --append last 11 months of Travel enquiries - AWS Source
insert into table customer_profile.CustDataSet 
Select 
lower(trim(p.email)) email, 
p.riskheaderid as Riskheaderid,
p.productcode as productcode, 
p.productname as productname, 
p.enquirydatetime as EnquiryDateTime, 
p.commencementdate as commencementdate, 
p.totalclickthroughs, 
p.totalshowdetails,
p.abicode,
p.commencementdate as GroupingDate,
concat_ws(
            '|',
			coalesce(lower(trim(p.email)), '#'),
			coalesce(p.commencementdate, '#'), 
			'tr',
			'#'
			) as grouping,
 concat_ws(
			'|',
			'email',
			'commencementdate', 
			'productcode',
			'abicode'
			) as grouping_desc
from redpoint.travelenquiry p
where p.lasttouchdate >= date_add(from_unixtime(unix_timestamp()),-335)
 ;
 
--flag each enquiry with Maximum sale date associated with riskheaderid
create table customer_profile.CustDataSet_WithSales as 
Select 
CustDataSet.email,
CustDataSet.riskheaderid, 
CustDataSet.productcode, 
CustDataSet.productname, 
CustDataSet.enquirydatetime,
CustDataSet.commencementdate,
allsales_history.transactiondate SaleDate,
date_add(allsales_history.transactiondate,335) ResolicitationDate,
case 
	when allsales_history.transactiondate is not null  then 1
	else 0 
end Sale,
allsales_history.brandname,
CustDataSet.totalclickthroughs, 
CustDataSet.totalshowdetails,
CustDataSet.abicode,
CustDataSet.GroupingDate,
CustDataSet.grouping,
CustDataSet.grouping_desc,
reflect('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', CustDataSet.grouping) as grouping_hash
from customer_profile.CustDataSet 
left join 
	(Select riskheaderid, productname, max(transactiondate) transactiondate, max(brandname) brandname from cds.cds_sales  group by riskheaderid,productname)
	allsales_history on trim(UPPER(allsales_history.riskheaderid)) = trim(UPPER(CustDataSet.riskheaderid))
and  trim(UPPER(allsales_history.productname))= trim(UPPER(CustDataSet.productname))
 ;

--flag each enquiry with "Has a claim" associated with riskheaderid
create table customer_profile.CustDataSet_WithClaims as 
Select 
CustDataSet_WithSales.email,
CustDataSet_WithSales.GroupingDate,
CustDataSet_WithSales.grouping,
    CustDataSet_WithSales.grouping_desc,
    CustDataSet_WithSales.grouping_hash,
CustDataSet_WithSales.riskheaderid, 
CustDataSet_WithSales.productcode, 
CustDataSet_WithSales.enquirydatetime,
CustDataSet_WithSales.commencementdate,
CustDataSet_WithSales.SaleDate,
CustDataSet_WithSales.ResolicitationDate,
CustDataSet_WithSales.Sale,
CustDataSet_WithSales.brandname,
CustDataSet_WithSales.totalclickthroughs,
CustDataSet_WithSales.totalshowdetails,
case 
	 when allclaims_history.rewardname is not null then 'Toy'
	 else 'None' end rewardname, 
CustDataSet_WithSales.abicode
from customer_profile.CustDataSet_WithSales
left join 
	(Select riskheaderid, productname, max(rewardname) rewardname from cds.cds_claims group by riskheaderid, productname) allclaims_history 
	on trim(UPPER(allclaims_history.riskheaderid)) = trim(UPPER(CustDataSet_WithSales.riskheaderid))
and  trim(UPPER(allclaims_history.productname))= trim(UPPER(CustDataSet_WithSales.productname))
;

-- Rebuild dataset to remove fragmentation
create table customer_profile.CustDataSet_WithClaims_tmp as 
select * from customer_profile.CustDataSet_WithClaims;

Drop table if exists customer_profile.CustDataSet_WithClaims;
ALTER TABLE CustDataSet_WithClaims_tmp RENAME TO CustDataSet_WithClaims;

--Create a set of unique hash keys for full enquiry data set
drop table if exists customer_profile.reward_uid_merge;

create table customer_profile.reward_uid_merge as
select reflect("java.util.UUID", "randomUUID") as reward_uid,
max(email) email,
    grouping,
    grouping_desc,
    grouping_hash,
    from_unixtime(unix_timestamp()) as datecreated,
    max(riskheaderid) as lastenquiry
from customer_profile.CustDataSet_WithClaims
group by grouping, grouping_desc, grouping_hash;
 
drop table if exists customer_profile.newreward_uid;
create  table customer_profile.newreward_uid as 
select 
distinct 
m.email
from customer_profile.reward_uid_merge m
left join customer_profile.reward_uid r
ON  m.grouping_hash = r.grouping_hash
Where r.uid IS NULL;


--Insert new hash key into reward UID historical table
insert into table customer_profile.reward_uid
select 
m.reward_uid,
m.email,
m.grouping,
    m.grouping_desc,
m.grouping_hash,
m.datecreated,
m.lastenquiry
from customer_profile.reward_uid_merge m
left join customer_profile.reward_uid r
ON  m.grouping_hash = r.grouping_hash
Where r.uid IS NULL;



 --Create a set of riskheaderids for reward uid (purchase opportunity)   

drop table if exists customer_profile.enquiry_uid_merge;

create table customer_profile.enquiry_uid_merge as
select
reflect("java.util.UUID", "randomUUID") as enquiry_uid,
e.email,
e.riskheaderid,
r.uid as reward_uid,
from_unixtime(unix_timestamp()) as datecreated
from customer_profile.CustDataSet_WithClaims e
left join customer_profile.reward_uid r
ON e.grouping_hash = r.grouping_hash;

--insert into historical copy of riskheaderids found within aggregated reward uid (purchase opportunity)
insert into table customer_profile.enquiry_uid
select 
m.enquiry_uid,
m.email,
m.riskheaderid,
        m.reward_uid,
m.datecreated
from customer_profile.enquiry_uid_merge m
left join customer_profile.enquiry_uid e
ON m.riskheaderid = e.riskheaderid
Where e.riskheaderid IS NULL;


-- Build Aggregated view of Enquiry for Customer Profile
create table customer_profile.CustDataSet_Rewards_PreProb as
select lower(trim(c.email)) email,
r.uid as PurchaseOpportunityID,
upper(trim(productcode)) productcode,
max(enquirydatetime) LastTouchDate,
max(SaleDate) SaleDate,
max(ResolicitationDate) ResolicitationDate,
max(rewardname) rewardname,
max(Sale) Sale,
max(brandname) brandname,
max(totalclickthroughs) totalclickthroughs, 
max(totalshowdetails) totalshowdetails
from customer_profile.CustDataSet_WithClaims c
left join customer_profile.reward_uid r
on c.grouping_hash = r.grouping_hash
group by lower(trim(c.email)),upper(trim(productcode)),r.uid;

--Append Probability and create final rewards profile of enquiries
drop table if exists customer_profile.CustDataSet_Rewards;
create table customer_profile.CustDataSet_Rewards as
select  lower(trim(pp.email)) email ,
pp.PurchaseOpportunityID,
pp.productcode,
pp.LastTouchDate,
pp.SaleDate,
pp.ResolicitationDate,
coalesce(prob.probability, -1066)  as probability,
rewardname,
case when pp.Sale = 1 then 'True' else 'False' end Sale,
pp.brandname
from customer_profile.CustDataSet_Rewards_PreProb pp
left join reference_tables.probability prob on 
pp.productcode = prob.productcode
and pp.totalclickthroughs = prob.clickthroughs
and pp.totalshowdetails = prob.showdetails
and pp.sale = prob.sale;
