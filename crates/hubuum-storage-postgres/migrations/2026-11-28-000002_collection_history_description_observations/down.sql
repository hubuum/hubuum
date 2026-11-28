SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

DROP TRIGGER refresh_large_collection_description ON public.collections_history;
DROP TRIGGER observe_large_collection_description ON public.collections_history;
DROP FUNCTION public.hubuum_refresh_collection_description_observation();
DROP FUNCTION public.hubuum_observe_collection_description(text);
DROP TABLE public.collection_description_digest_history;
