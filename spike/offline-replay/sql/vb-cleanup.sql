DROP TABLE IF EXISTS public.t_vbspike;
SELECT pg_drop_replication_slot('vb_offline_spike');
SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'vb_offline_spike';
