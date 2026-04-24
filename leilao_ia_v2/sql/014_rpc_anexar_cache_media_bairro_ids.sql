-- RPC atómica para anexar IDs em ``cache_media_bairro_ids`` sem duplicar.
-- Uso esperado no backend Python:
--   rpc_anexar_cache_media_bairro_ids(p_imovel_id := <uuid>, p_novos_ids := ARRAY['uuid1','uuid2'])

create or replace function public.rpc_anexar_cache_media_bairro_ids(
    p_imovel_id uuid,
    p_novos_ids text[]
)
returns table(cache_media_bairro_ids text[])
language sql
security definer
set search_path = public
as $$
with alvo as (
    select
        li.id,
        coalesce(li.cache_media_bairro_ids::text[], array[]::text[]) as cur_ids
    from public.leilao_imoveis li
    where li.id = p_imovel_id
),
add_ids as (
    select distinct trim(x)::text as id
    from unnest(coalesce(p_novos_ids, array[]::text[])) as x
    where trim(x) <> ''
),
merged as (
    select
        a.id,
        coalesce(
            (
                select array_agg(v.id order by v.ord)
                from (
                    select c.id, c.ord
                    from unnest(a.cur_ids) with ordinality as c(id, ord)
                    union all
                    select ad.id, 1000000 + row_number() over (order by ad.id)
                    from add_ids ad
                    where not (ad.id = any(a.cur_ids))
                ) v
            ),
            array[]::text[]
        ) as ids
    from alvo a
),
upd as (
    update public.leilao_imoveis li
    set cache_media_bairro_ids = m.ids::uuid[]
    from merged m
    where li.id = m.id
    returning li.cache_media_bairro_ids::text[] as cache_media_bairro_ids
)
select u.cache_media_bairro_ids from upd u;
$$;

comment on function public.rpc_anexar_cache_media_bairro_ids(uuid, text[]) is
'Anexa cache_media_bairro_ids de forma atómica sem duplicar, preservando ordem atual e adicionando novos IDs ao fim.';
