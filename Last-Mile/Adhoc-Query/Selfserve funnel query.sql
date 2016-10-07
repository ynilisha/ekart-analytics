select lookup_date(reschedule_trigger_time) as reschedule_trigger_date,
sum(case when lookup_time(reschedule_trigger_time)>=700 and lookup_time(reschedule_trigger_time)<2100 then 1 else 0 end) as Sms_trigger_morning_cnt,
sum(case when lookup_time(reschedule_trigger_time)>=700 and lookup_time(reschedule_trigger_time)<2100 and lookup_time(reschedule_landing_time) is not null then 1 else 0 end) as reschedule_landing_morning_cnt,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(reschedule_confirmation_time) is not null then 1 else 0 end) as reschedule_confirmation_morning_cnt,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(reschedule_confirmation_time) is not null and shipment_delivered_at_date_key is not null then 1 else 0 end) as reschedule_delivered_morning,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(acn_landing_time) is not null then 1 else 0 end) as acn_landing_morning_cnt,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(acn_confirmation_time) is not null then 1 else 0 end) as acn_confirmation_morning_cnt,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(acn_confirmation_time) is not null and shipment_delivered_at_date_key is not null then 1 else 0 end) as acn_delivered_morning,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(cancellation_landing_time) is not null then 1 else 0 end) as cancel_landing_morning_cnt,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(cancellation_confirmation_time) is not null then 1 else 0 end) as cancel_confirm_morning_cnt,
sum(case when lookup_time(reschedule_trigger_time)>=700 and  lookup_time(reschedule_trigger_time)<2100 and lookup_time(cancellation_confirmation_time) is not null and shipment_delivered_at_date_key is not null then 1 else 0 end) as cancel_delivered_morning,
sum(case when lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700 then 1 else 0 end) as sms_trigger_night_cnt,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(reschedule_landing_time) is not null then 1 else 0 end) as reschedule_landing_night_cnt,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(reschedule_confirmation_time) is not null then 1 else 0 end) as reschedule_confirmation_night_cnt,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(reschedule_confirmation_time) is not null and shipment_delivered_at_date_key is not null then 1 else 0 end) as reschedule_delivered_night,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(acn_landing_time) is not null then 1 else 0 end) as acn_landing_night_cnt,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(acn_confirmation_time) is not null then 1 else 0 end) as acn_confirmation_night_cnt,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(acn_confirmation_time) is not null and shipment_delivered_at_date_key is not null then 1 else 0 end) as acn_delivered_night,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(cancellation_landing_time) is not null then 1 else 0 end) as cancel_landing_night_cnt,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(cancellation_confirmation_time) is not null then 1 else 0 end) as cancel_confirm_night_cnt,
sum(case when (lookup_time(reschedule_trigger_time)>=2100 or lookup_time(reschedule_trigger_time)<700) and lookup_time(cancellation_confirmation_time) is not null and shipment_delivered_at_date_key is not null then 1 else 0 end) as cancel_delivered_night,
sum(case when reschedule_trigger_time is not null and reschedule_confirmation_time is null and last_undelivery_status='Undelivered_Request_For_Reschedule' then 1 else 0 end) as  reschedules_requested_offline,
sum(case when reschedule_trigger_time is not null and reschedule_confirmation_time is null and last_undelivery_status='Undelivered_No_Response' then 1 else 0 end) as  customer_not_responsding,
sum(case when reschedule_trigger_time is not null and reschedule_confirmation_time is null and last_undelivery_status='Undelivered_Order_Rejected_By_Customer' then 1 else 0 end) as doorstep_cancellations
from
(select  
max(tasklist_id) as tasklist_id,
max(shipment_last_received_dh_id_key) as  shipment_last_received_dh_id_key,
max(cancel_comment) as cancel_comment,
max(cancel_reason) as cancel_reason,
max(alternate_contact_number) as alternate_contact_number,
max(RESCHEDULE_shipment_status) as RESCHEDULE_shipment_status,
max(RESCHEDULE_updater) as RESCHEDULE_updater,
max(ACN_shipment_status) as ACN_shipment_status,
max(ACN_updater) as ACN_updater,
max(Cancel_shipment_status) as Cancel_shipment_status,
max(Cancel_updater) as Cancel_updater,
max(primary_contact_number) as primary_contact_number,
product_title,
merchant_brand_name,
vendor_tracking_id,
max(expected_delivery_date) as expected_delivery_date,
max(rescheduled_delivery_date) as rescheduled_delivery_date,
max(Cancellation_trigger_time) as cancellation_trigger_time,
max(Cancellation_landing_time) as cancellation_landing_time,
max(Cancellation_confirmation_time) as cancellation_confirmation_time,
max(ACN_trigger_time) as acn_trigger_time,
max(ACN_landing_time) as ACN_landing_time,
max(ACN_confirmation_time) as acn_confirmation_time,
max(RESCHEDULE_trigger_time) as reschedule_trigger_time,
max(RESCHEDULE_landing_time) as reschedule_landing_time,
max(RESCHEDULE_confirmation_time) as reschedule_confirmation_time,
max(shipment_delivered_at_date_key) as shipment_delivered_at_date_key,
max(last_undelivery_status) as last_undelivery_status
from
(select 
vendor_tracking_id,
tasklist_id,
request_type,
event_type,
if(shipment_last_received_dh_id_key !=0,shipment_last_received_dh_id_key,NULL) as shipment_last_received_dh_id_key,
hub_name,
city,zone,
expected_delivery_date,
primary_contact_number,
update_date_time,
shipment_status,
updater,
product_title,
merchant_brand_name,shipment_delivered_at_date_key,
last_undelivery_status,
if(request_type="RESCHEDULE" and event_type="CONFIRMATION",rescheduled_delivery_date,NULL) as rescheduled_delivery_date,
if(request_type="CANCELLATION" and event_type="TRIGGER",update_date_time,NULL) as Cancellation_trigger_time,
if(request_type="CANCELLATION" and event_type="LANDING",update_date_time,NULL) as Cancellation_landing_time,
if(request_type="CANCELLATION" and event_type="CONFIRMATION",update_date_time,NULL) as Cancellation_confirmation_time,
if(request_type="ALTERNATE_CONTACT_NUMBER" and event_type="TRIGGER",update_date_time,NULL) as ACN_trigger_time,
if(request_type="ALTERNATE_CONTACT_NUMBER" and event_type="LANDING",update_date_time,NULL) as ACN_landing_time,
if(request_type="ALTERNATE_CONTACT_NUMBER" and event_type="CONFIRMATION",update_date_time,NULL) as ACN_confirmation_time,
if(request_type="RESCHEDULE" and event_type="TRIGGER",update_date_time,NULL) as RESCHEDULE_trigger_time,
if(request_type="RESCHEDULE" and event_type="LANDING",update_date_time,NULL) as RESCHEDULE_landing_time,
if(request_type="RESCHEDULE" and event_type="CONFIRMATION",update_date_time,NULL) as RESCHEDULE_confirmation_time,
if(request_type="CANCELLATION" and event_type="CONFIRMATION",cancel_comment,NULL) as cancel_comment,
if(request_type="CANCELLATION" and event_type="CONFIRMATION",cancel_reason,NULL) as cancel_reason,
if(request_type="ALTERNATE_CONTACT_NUMBER" and event_type="CONFIRMATION",alternate_contact_number,NULL) as alternate_contact_number,
if(request_type="RESCHEDULE" and event_type="CONFIRMATION",shipment_status,NULL) as RESCHEDULE_shipment_status,
if(request_type="RESCHEDULE" and event_type="CONFIRMATION",updater,NULL) as RESCHEDULE_updater,
if(request_type="ALTERNATE_CONTACT_NUMBER" and event_type="CONFIRMATION",shipment_status,NULL) as ACN_shipment_status,
if(request_type="ALTERNATE_CONTACT_NUMBER" and event_type="CONFIRMATION",updater,NULL) as ACN_updater,
if(request_type="CANCELLATION" and event_type="CONFIRMATION",shipment_status,NULL) as Cancel_shipment_status,
if(request_type="CANCELLATION" and event_type="CONFIRMATION",updater,NULL) as Cancel_updater
from (select 
vendor_tracking_id,
shipment_last_received_dh_id_key,
hub_name,
city,
zone,
misroute_flag,
tasklist_id,
request_type,
event_type,
cancel_comment,
cancel_reason,
expected_delivery_date,
rescheduled_delivery_date,
alternate_contact_number,
primary_contact_number,
shipment_status,
updater,
product_title,
merchant_brand_name,
shipment_delivered_at_date_key,
update_date_time,
last_undelivery_status
from(select 
shipment_last_received_dh_id_key,
dim.name as hub_name,
dim.city,
dim.zone,
fact.misroute_flag,
self.vendor_tracking_id,
self.tasklist_id,
self.request_type,
self.event_type,
self.cancel_comment,
self.cancel_reason,
self.expected_delivery_date,
self.rescheduled_delivery_date,
self.alternate_contact_number,
self.primary_contact_number,
self.shipment_status,
self.updater,
self.product_title,
self.merchant_brand_name,
fact.shipment_delivered_at_date_key,
fact.last_undelivery_status,
row_number() over(partition by self.vendor_tracking_id,self.tasklist_id,self.request_type,self.event_type order by  update_date_time desc) as rnk,
self.update_date_time
from
(select
`data`.shipment_id as vendor_tracking_id
,concat("Runsheet-",`data`.runsheet_id) as tasklist_id
,`data`.request_type
,`data`.cancel_comment
,`data`.expected_delivery_date
,`data`.product_title
,`data`.rescheduled_delivery_date
,`data`.updater
,`data`.cancel_reason
,`data`.event_type
,`data`.alternate_contact_number
,`data`.primary_contact_number
,`data`.merchant_brand_name
,`data`.update_date_time
,`data`.shipment_status
from bigfoot_journal.dart_wsr_scp_ekl_selfserveservice_1) self
left outer join bigfoot_external_neo.scp_ekl__shipment_hive_90_fact fact 
on self.vendor_tracking_id=fact.vendor_tracking_id 
left outer join bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim dim
on fact.shipment_last_received_dh_id_key=dim.ekl_hive_facility_dim_key)a
where rnk=1 )a) a
group by vendor_tracking_id,
product_title,
merchant_brand_name) a
group by lookup_date(reschedule_trigger_time)
having lookup_date(reschedule_trigger_time) is not null and lookup_date(reschedule_trigger_time) between 20160910 and 20160920;