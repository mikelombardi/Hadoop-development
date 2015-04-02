use customer_profile;

--Remove all MyCTM Duplicates - this table will hold all duplicated emails
drop table if exists customer_profile.MycTMDupes_ALL;
Create table customer_profile.MycTMDupes_ALL as
select email from 
(
select count(*) c0, email, lower(max(coalesce(haspassword,'False')))  haspass,lower(min(coalesce(haspassword,'False')))  minhaspass,
max(accountid) maxaccidd, min(accountid) minid
from myctm.myctm_accounts
group by email
having c0 >1
) dupes;


drop table if exists customer_profile.MycTMDupes_CleanEmails;
create table customer_profile.MycTMDupes_CleanEmails as
select email from 
(
select count(distinct lower(coalesce(haspassword,'False'))) c0, 
  count(*) totalcount, email
from myctm.myctm_accounts
where lower(coalesce(haspassword,'False')) = 'true'
group by email
  having c0 = 1
) dupes
where c0 = totalcount ;

drop table if exists customer_profile.MycTMDupes;
Create table customer_profile.MycTMDupes as 
Select MycTMDupes_ALL.email 
from customer_profile.MycTMDupes_ALL
left join customer_profile.MycTMDupes_CleanEmails on MycTMDupes_ALL.email = MycTMDupes_CleanEmails.email
where MycTMDupes_CleanEmails.email is null;


--Insert MyCTM accounts and primary associate
drop table if exists customer_profile.customer_account_ParseA;
create table customer_profile.customer_account_ParseA  as 
select 
	'myctm' as source,
	a.accountid,
       a.bin_accountid,
a.net_accountid,
	u.associateid,
	lower(trim(a.email)) email,
	u.firstname,
	u.surname,
	u.title,
	u.gender,
	u.dateofbirth, 
	u.maritalstatus,
	a.emailoptin, 
	a.phoneoptin, 
	a.smsoptin, 
	a.postoptin, 
	u.lineone as addresspostallineone, 
	u.linetwo as addresspostallinetwo, 
	u.linethree as addresspostallinethree,
	u.linefour as addresspostallinefour,
    u.linefive as addresspostallinefive,
	u.linesix as addresspostallinesix,
	u.lineseven as addresspostallineseven,
	u.postcode,
	a.haspassword,
	cast(a.lasttouchdate as string) as lasttouchdate
from myctm.myctm_accounts a
left join myctm.myctm_associates u
	on a.primaryassociateid = u.associateid
left join customer_profile.MycTMDupes
  on lower(trim(MycTMDupes.email)) = lower(trim(a.email))
where lower(a.haspassword) = 'true' and 
MycTMDupes.email is null;



--insert Speedtrap accounts where the email is not in previous query
insert into table customer_profile.customer_account_ParseA
select 
	'SpeedTrap' as source,
	cast(NULL as string) accountid,
       cast(NULL as string) bin_accountid,
cast(NULL as string) net_accountid,
	cast(NULL as string) associateid,
	lower(trim(ca.email)) as email,
	cast(NULL as string) as firstname,
	cast(NULL as string) as surname,
	cast(NULL as string) title,
	cast(NULL as string) gender,
	cast(NULL as string) as dateofbirth,
	cast(NULL as string) maritalstatus,
	cast(NULL as string) as emailoptin,
	cast(NULL as string) as phoneoptin, 
	cast(NULL as string) as smsoptin, 
	cast(NULL as string) as postoptin,
	cast(NULL as string) addresspostallineone, 
	cast(NULL as string) addresspostallinetwo, 
	cast(NULL as string) addresspostallinethree,
	cast(NULL as string) addresspostallinefour,
	cast(NULL as string) addresspostallinefive,
	cast(NULL as string) addresspostallinesix,
	cast(NULL as string) addresspostallineseven,
	cast(NULL as string) postcode,
	cast(NULL as string) haspassword,
	ca.lasttouchdate as lasttouchdate
from (Select trim(lower(email)) email, max(lasttouchdate) lasttouchdate  from speedtrap.SpeedTrap_Summary group by trim(lower(email)))  ca
LEFT JOIN customer_profile.customer_account_ParseA ma
	on lower(trim(ma.email)) = lower(trim(ca.email))
where ma.email is null;

--insert Money accounts where the email is not in previous query
insert into table customer_profile.customer_account_ParseA
select 
	'Money' as source,
	cast(NULL as string) accountid,
       cast(NULL as string) bin_accountid,
cast(NULL as string) net_accountid,
	cast(NULL as string) associateid,
	lower(trim(ca.email)) as email,
	cast(NULL as string) as firstname,
	cast(NULL as string) as surname,
	cast(NULL as string) title,
	cast(NULL as string) gender,
	cast(NULL as string) as dateofbirth,
	cast(NULL as string) maritalstatus,
	cast(NULL as string) as emailoptin,
	cast(NULL as string) as phoneoptin, 
	cast(NULL as string) as smsoptin, 
	cast(NULL as string) as postoptin,
	cast(NULL as string) addresspostallineone, 
	cast(NULL as string) addresspostallinetwo, 
	cast(NULL as string) addresspostallinethree,
	cast(NULL as string) addresspostallinefour,
	cast(NULL as string) addresspostallinefive,
	cast(NULL as string) addresspostallinesix,
	cast(NULL as string) addresspostallineseven,
	cast(NULL as string) postcode,
	cast(NULL as string) haspassword,
	ca.lasttouchdate as lasttouchdate
from (Select trim(lower(email)) email, max(lasttouchdate) lasttouchdate  from money.money group by  trim(lower(email)))  ca
LEFT JOIN customer_profile.customer_account_ParseA ma
	on lower(trim(ma.email)) = lower(trim(ca.email))
where ma.email is null;


-- Build Travel Accounts
drop table if exists customer_profile.customer_account_travel;
create table customer_profile.customer_account_travel as
select 
	'travel' as source,
	accounts.email,
	accounts.firstname,
	accounts.surname,
	cast(NULL as string) as dateofbirth,
	accounts.emailoptin,
	cast(NULL as string) as phoneoptin, 
	cast(NULL as string) as smsoptin, 
	cast(NULL as string) as postoptin,
    accounts.lasttouchdate
 from
 (select 
		p.email,
		p.firstname as firstname,
		p.surname as surname,
		p.emailoptin,
		p.lasttouchdate
	from ctm_travel.travel_accounts p
) accounts
;

-- Combine and Rank non MyCTM Accounts
drop table if exists customer_profile.customer_account_merged;
create table customer_profile.customer_account_merged as
select
	source,
	email,
	firstname,
	surname,
	dateofbirth,
	emailoptin,
	phoneoptin, 
	smsoptin, 
	postoptin,
	lasttouchdate,
	ROW_NUMBER() OVER (PARTITION BY email  ORDER BY lasttouchdate desc) as RowNum
from
(
	select 
		source,
		email,
		firstname,
		surname,
		dateofbirth,
		emailoptin,
		phoneoptin, 
		smsoptin, 
		postoptin,
		lasttouchdate
	from customer_profile.customer_account_travel -- New Travel Accounts
	UNION ALL
	select 
		'cds' as source,
		email,
		firstname,
		surname,
		dateofbirth,
		emailoptin,
		phoneoptin, 
		smsoptin, 
		postoptin,
		regexp_replace(lasttouchdate,'-','') as lasttouchdate
	from cds.cds_accounts -- CDS Accounts
) accounts
;


--Merge Previous MyCTM with Non-MyCTM Accounts - creates a single account table with MYCTM, Speedtrap, money, travel and CDS
Drop  table if exists customer_profile.customer_account_ParseA_Merge;
Create table customer_profile.customer_account_ParseA_Merge as
Select * from 
(
select
	coalesce(ma.source, ca.source) as source,
	ma.accountid,
    ma.bin_accountid,
    ma.net_accountid,
	ma.associateid,
	coalesce(lower(trim(ma.email)), ca.email) as email,
	coalesce(ma.firstname, ca.firstname) as firstname,
	coalesce(ma.surname, ca.surname) as surname,
	ma.title,
	ma.gender,
	coalesce(ma.dateofbirth, ca.dateofbirth) as dateofbirth,
	ma.maritalstatus,
	-- Take opt in options from non MyCTM accounts as preference
	coalesce(ca.emailoptin, ma.emailoptin) as emailoptin, 
	coalesce(ca.phoneoptin, ma.phoneoptin) as phoneoptin, 
	coalesce(ca.smsoptin, ma.smsoptin) as smsoptin, 
	coalesce(ca.postoptin, ma.postoptin) as postoptin,
	ma.addresspostallineone, 
	ma.addresspostallinetwo, 
	ma.addresspostallinethree,
	ma.addresspostallinefour,
	ma.addresspostallinefive,
	ma.addresspostallinesix,
	ma.addresspostallineseven,
	ma.postcode,
	ma.haspassword,
	coalesce(ma.lasttouchdate, ca.lasttouchdate) as lasttouchdate
from customer_profile.customer_account_ParseA ma
inner join customer_profile.customer_account_merged ca
	on lower(trim(ma.email)) = ca.email
where ca.rownum = 1 -- Latest entry 
UNION ALL
select 
	ca.source,
	cast(NULL as string) accountid,
    cast(NULL as string) bin_accountid,
	cast(NULL as string) net_accountid,
	cast(NULL as string) associateid,
	ca.email as email,
	ca.firstname as firstname,
	ca.surname as surname,
	cast(NULL as string) title,
	cast(NULL as string) gender,
	ca.dateofbirth as dateofbirth,
	cast(NULL as string)maritalstatus,
	ca.emailoptin as emailoptin,
	ca.phoneoptin as phoneoptin, 
	ca.smsoptin as smsoptin, 
	ca.postoptin as postoptin,
	cast(NULL as string) addresspostallineone, 
	cast(NULL as string) addresspostallinetwo, 
	cast(NULL as string) addresspostallinethree,
	cast(NULL as string) addresspostallinefour,
	cast(NULL as string) addresspostallinefive,
	cast(NULL as string) addresspostallinesix,
	cast(NULL as string) addresspostallineseven,
	cast(NULL as string) postcode,
	cast(NULL as string) haspassword,
	ca.lasttouchdate as lasttouchdate
from customer_profile.customer_account_merged ca
left join customer_profile.customer_account_ParseA ma
	on lower(trim(ma.email)) = ca.email
where ma.email is null -- No MyCTM Account Present
and ca.rownum = 1 -- Latest entry 
) Accounts
;



--Hash Creation across Profile Account table
drop table if exists customer_profile.customer_account_ParseA_Hash;
Create table customer_profile.customer_account_ParseA_Hash as 
Select source,
	accountid,
       bin_accountid,
       net_accountid,
	associateid,
	email,
	lower(firstname) as firstname,
	lower(surname) as surname,
	title,
	gender,
	dateofbirth,
	maritalstatus,
	emailoptin,
	phoneoptin, 
	smsoptin, 
	postoptin,
	addresspostallineone, 
	addresspostallinetwo, 
	addresspostallinethree,
	addresspostallinefour,
	addresspostallinefive,
	addresspostallinesix,
	addresspostallineseven,
	postcode,
	haspassword,
	lasttouchdate
,reflect('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', 
	concat_WS('~',
		COALESCE(lower(trim(source)),'#'),
		COALESCE(lower(trim(accountid)),'#'),
		COALESCE(lower(trim(associateid)),'#'),
		COALESCE(lower(trim(email)),'#'),
		COALESCE(lower(trim(firstname)),'#'),
		COALESCE(lower(trim(surname)),'#'),
		COALESCE(lower(trim(title)),'#'),
		COALESCE(lower(trim(gender)),'#'),
		COALESCE(lower(trim(dateofbirth)),'#'),
		COALESCE(lower(trim(maritalstatus)),'#'),
		COALESCE(lower(trim(emailoptin)),'#'),
		COALESCE(lower(trim(phoneoptin)),'#'), 
		COALESCE(lower(trim(smsoptin)),'#'), 
		COALESCE(lower(trim(postoptin)),'#'),
		COALESCE(lower(trim(addresspostallineone)),'#'), 
		COALESCE(lower(trim(addresspostallinetwo)),'#'), 
		COALESCE(lower(trim(addresspostallinethree)),'#'),
		COALESCE(lower(trim(addresspostallinefour)),'#'),
		COALESCE(lower(trim(addresspostallinefive)),'#'),
		COALESCE(lower(trim(addresspostallinesix)),'#'),
		COALESCE(lower(trim(addresspostallineseven)),'#'),
		COALESCE(lower(trim(postcode)),'#'),
		COALESCE(lower(trim(haspassword)),'#'),
		COALESCE(lower(trim(lasttouchdate)),'#')
	)) as MatchingHash
from customer_profile.customer_account_ParseA_Merge;


drop table if exists customer_profile.EnquiryChangedRecords;
Create table customer_profile.EnquiryChangedRecords as
select distinct EnquiryChange.email
from  customer_profile.newreward_uid  EnquiryChange;


--Create ParseB Table (will be renamed to customer_account at the end of the process)
--Will hold New, Updated and Unchanged accounts
drop  table if exists customer_profile.customer_account_ParseB;
Create table customer_profile.customer_account_ParseB as
Select 
    reflect("java.util.UUID", "randomUUID") DocUID,
	'NEW' as Movement,
	from_unixtime(unix_timestamp()) as MovementDate,
	from_unixtime(unix_timestamp()) as CreateDate,
	reflect("java.util.UUID", "randomUUID") UniqueID,
	customer_account_ParseA_Hash.source,
	customer_account_ParseA_Hash.accountid,
customer_account_ParseA_Hash.bin_accountid,
customer_account_ParseA_Hash.net_accountid,
	customer_account_ParseA_Hash.associateid,
	customer_account_ParseA_Hash.email,
	customer_account_ParseA_Hash.firstname,
	customer_account_ParseA_Hash.surname,
	customer_account_ParseA_Hash.title,
	customer_account_ParseA_Hash.gender,
	customer_account_ParseA_Hash.dateofbirth,
	customer_account_ParseA_Hash.maritalstatus,
	customer_account_ParseA_Hash.emailoptin,
	customer_account_ParseA_Hash.phoneoptin, 
	customer_account_ParseA_Hash.smsoptin, 
	customer_account_ParseA_Hash.postoptin,
	customer_account_ParseA_Hash.addresspostallineone, 
	customer_account_ParseA_Hash.addresspostallinetwo, 
	customer_account_ParseA_Hash.addresspostallinethree,
	customer_account_ParseA_Hash.addresspostallinefour,
	customer_account_ParseA_Hash.addresspostallinefive,
	customer_account_ParseA_Hash.addresspostallinesix,
	customer_account_ParseA_Hash.addresspostallineseven,
	customer_account_ParseA_Hash.postcode,
	customer_account_ParseA_Hash.haspassword,
	customer_account_ParseA_Hash.lasttouchdate,
    customer_account_ParseA_Hash.MatchingHash
from customer_profile.customer_account_ParseA_Hash
left join customer_profile.customer_account
	on lower(trim(customer_account.email)) = lower(trim(customer_account_ParseA_Hash.email))
where customer_account.email is null;



--Insert unchanged customer accounts
drop table if exists customer_profile.customer_account_ParseB_UnchangedRecords;
create table customer_profile.customer_account_ParseB_UnchangedRecords as
Select 
    customer_account.DocUID,
	customer_account.MovementDate,
	customer_account.CreateDate,
	customer_account.UniqueID,
	customer_account.source,
	customer_account.accountid,
	customer_account.bin_accountid,
	customer_account.net_accountid,
	customer_account.associateid,
	customer_account.email,
	customer_account.firstname,
	customer_account.surname,
	customer_account.title,
	customer_account.gender,
	customer_account.dateofbirth,
	customer_account.maritalstatus,
	customer_account.emailoptin,
	customer_account.phoneoptin, 
	customer_account.smsoptin, 
	customer_account.postoptin,
	customer_account.addresspostallineone, 
	customer_account.addresspostallinetwo, 
	customer_account.addresspostallinethree,
	customer_account.addresspostallinefour,
	customer_account.addresspostallinefive,
	customer_account.addresspostallinesix,
	customer_account.addresspostallineseven,
	customer_account.postcode,
	customer_account.haspassword,
	customer_account.lasttouchdate,
    customer_account.MatchingHash
from customer_profile.customer_account
join customer_profile.customer_account_ParseA_Hash
	on customer_account.MatchingHash = customer_account_ParseA_Hash.MatchingHash
    and lower(trim(customer_account.email)) = lower(trim(customer_account_ParseA_Hash.email));


insert into table customer_profile.customer_account_ParseB 
Select 
    customer_account.DocUID,
	'NO CHANGE' as Movement,
	customer_account.MovementDate,
	customer_account.CreateDate,
	customer_account.UniqueID,
	customer_account.source,
	customer_account.accountid,
	customer_account.bin_accountid,
	customer_account.net_accountid,
	customer_account.associateid,
	customer_account.email,
	customer_account.firstname,
	customer_account.surname,
	customer_account.title,
	customer_account.gender,
	customer_account.dateofbirth,
	customer_account.maritalstatus,
	customer_account.emailoptin,
	customer_account.phoneoptin, 
	customer_account.smsoptin, 
	customer_account.postoptin,
	customer_account.addresspostallineone, 
	customer_account.addresspostallinetwo, 
	customer_account.addresspostallinethree,
	customer_account.addresspostallinefour,
	customer_account.addresspostallinefive,
	customer_account.addresspostallinesix,
	customer_account.addresspostallineseven,
	customer_account.postcode,
	customer_account.haspassword,
	customer_account.lasttouchdate,
    customer_account.MatchingHash
from customer_profile.customer_account_ParseB_UnchangedRecords customer_account
left join customer_profile.EnquiryChangedRecords  EnquiryChangedRecords
    on EnquiryChangedRecords.email =  lower(trim(customer_account.email))
where EnquiryChangedRecords.email is null
;

insert into table customer_profile.customer_account_ParseB 
Select 
    customer_account.DocUID,
	'UPDATE Enq' as Movement,
	customer_account.MovementDate,
	customer_account.CreateDate,
	customer_account.UniqueID,
	customer_account.source,
	customer_account.accountid,
	customer_account.bin_accountid,
	customer_account.net_accountid,
	customer_account.associateid,
	customer_account.email,
	customer_account.firstname,
	customer_account.surname,
	customer_account.title,
	customer_account.gender,
	customer_account.dateofbirth,
	customer_account.maritalstatus,
	customer_account.emailoptin,
	customer_account.phoneoptin, 
	customer_account.smsoptin, 
	customer_account.postoptin,
	customer_account.addresspostallineone, 
	customer_account.addresspostallinetwo, 
	customer_account.addresspostallinethree,
	customer_account.addresspostallinefour,
	customer_account.addresspostallinefive,
	customer_account.addresspostallinesix,
	customer_account.addresspostallineseven,
	customer_account.postcode,
	customer_account.haspassword,
	customer_account.lasttouchdate,
    customer_account.MatchingHash
from customer_profile.customer_account_ParseB_UnchangedRecords customer_account
left join customer_profile.EnquiryChangedRecords  EnquiryChangedRecords
    on EnquiryChangedRecords.email =  lower(trim(customer_account.email))
where EnquiryChangedRecords.email is not null
;


--Create Updated Records
drop table if exists customer_profile.customer_account_ParseB_UpdatedRecords;
create table customer_profile.customer_account_ParseB_UpdatedRecords as
Select 
    customer_account_ParseA_Hash.*
from customer_profile.customer_account_ParseA_Hash
left join customer_profile.customer_account_ParseB
	on lower(trim(customer_account_ParseB.email)) = lower(trim(customer_account_ParseA_Hash.email))
where customer_account_ParseB.email is null;

drop table if exists customer_profile.customer_account_ParseC ;
Create table customer_profile.customer_account_ParseC as 
select * from customer_profile.customer_account_ParseB ;


drop table if exists customer_profile.customer_account_ParseB ;
Create table customer_profile.customer_account_ParseB as 
select * from customer_profile.customer_account_ParseC ;


--insert updated customer account records
insert into table customer_profile.customer_account_ParseB 
Select 
    reflect("java.util.UUID", "randomUUID") DocUID,
	'UPDATE' as Movement,
	from_unixtime(unix_timestamp()) as MovementDate,
	customer_account.CreateDate, 
	customer_account.UniqueID,
	customer_account_ParseA_Hash.source,
	customer_account_ParseA_Hash.accountid,
customer_account_ParseA_Hash.bin_accountid,
customer_account_ParseA_Hash.net_accountid,
	customer_account_ParseA_Hash.associateid,
	customer_account.email,
	customer_account_ParseA_Hash.firstname,
	customer_account_ParseA_Hash.surname,
	customer_account_ParseA_Hash.title,
	customer_account_ParseA_Hash.gender,
	customer_account_ParseA_Hash.dateofbirth,
	customer_account_ParseA_Hash.maritalstatus,
	customer_account_ParseA_Hash.emailoptin,
	customer_account_ParseA_Hash.phoneoptin, 
	customer_account_ParseA_Hash.smsoptin, 
	customer_account_ParseA_Hash.postoptin,
	customer_account_ParseA_Hash.addresspostallineone, 
	customer_account_ParseA_Hash.addresspostallinetwo, 
	customer_account_ParseA_Hash.addresspostallinethree,
	customer_account_ParseA_Hash.addresspostallinefour,
	customer_account_ParseA_Hash.addresspostallinefive,
	customer_account_ParseA_Hash.addresspostallinesix,
	customer_account_ParseA_Hash.addresspostallineseven,
	customer_account_ParseA_Hash.postcode,
	customer_account_ParseA_Hash.haspassword,
	customer_account_ParseA_Hash.lasttouchdate,
    customer_account_ParseA_Hash.MatchingHash
from customer_profile.customer_account_ParseB_UpdatedRecords customer_account_ParseA_Hash
left join customer_profile.customer_account
	on lower(trim(customer_account.email)) = lower(trim(customer_account_ParseA_Hash.email))
where customer_account.email is not null;
