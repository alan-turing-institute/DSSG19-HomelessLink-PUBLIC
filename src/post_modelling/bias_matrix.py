from utils.utils import *


def run_query_age(k, model, environs):
    _, c = connect_cursor(environs)
    c.execute("""
    create view results.bmak{1}model{0} as (
    select
    --fn_gs
    not_known_fn_gs::float/s25_50_fn_gs::float as not_known_fn_gs_rate,
    under_25_fn_gs::float/s25_50_fn_gs::float as under_25_fn_gs_rate,
    over_50_fn_gs::float/s25_50_fn_gs::float as over_50_fn_gs_rate,

    --fn_pn
    not_known_fn_pn::float/s25_50_fn_pn::float as not_known_fn_pn_rate,
    under_25_fn_pn::float/s25_50_fn_pn::float as under_25_fn_pn_rate,
    over_50_fn_pn::float/s25_50_fn_pn::float as over_50_fn_pn_rate,

    --fn_lp
    not_known_fn_lp::float/s25_50_fn_lp::float as not_known_fn_lp_rate,
    under_25_fn_lp::float/s25_50_fn_lp::float as under_25_fn_lp_rate,
    over_50_fn_lp::float/s25_50_fn_lp::float as over_50_fn_lp_rate,

    --false negatives
    sum_not_known_fn::float/sum_25_50_fn::float as not_known_fn_rate,
    sum_under_25_fn::float/sum_25_50_fn::float as under_25_fn_rate,
    sum_over_50_fn::float/sum_25_50_fn::float as over_50_fn_rate,

    --after k
    sum_not_known_after_k::float/sum_25_50_after_k::float as not_known_after_k_rate,
    sum_under_25_after_k::float/sum_25_50_after_k::float as under_25_after_k_rate,
    sum_over_50_after_k::float/sum_25_50_after_k::float as s25_50_after_k_rate


    FROM (
    select
    -- false negatives
    d.sum_not_known_FN,
    d.sum_under_25_FN,
    d.sum_25_50_FN,
    d.sum_over_50_FN,

    --sum after k
    d.sum_not_known_after_k,
    d.sum_under_25_after_k,
    d.sum_25_50_after_k,
    d.sum_over_50_after_k,

    --false negatives / group size from semantic.alerts_with_features
    d.sum_25_50_FN::float/(select count(e.age_group)
    from (
    select a.score, b.age_group from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and age_group = '25 - 50 years old') e ) as s25_50_FN_GS,

    d.sum_under_25_FN::float/(select count(e.age_group)
    from (
    select a.score, b.age_group from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and age_group = '<25 years old') e ) as under_25_FN_GS,

    d.sum_over_50_FN::float/(select count(e.age_group)
    from (
    select a.score, b.age_group from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and age_group = '> 50 years old') e ) as over_50_FN_GS,

    d.sum_not_known_FN::float/(select count(e.age_group)
    from (
    select a.score, b.age_group from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and age_group = 'not known') e ) as not_known_FN_GS,



    -- false negatives / predicted negatives
    d.sum_not_known_FN::float/d.sum_not_known_after_k::float as not_known_FN_PN,
    d.sum_under_25_FN::float / d.sum_under_25_after_k::float as under_25_FN_PN,
    d.sum_25_50_FN::float/d.sum_25_50_after_k::float as s25_50_FN_PN,
    d.sum_over_50_FN::float/d.sum_over_50_after_k::float as over_50_FN_PN,

    -- false negatives / total positive within fold (entire list - not broken by k)
    d.sum_under_25_FN::float /(select sum(case when c.age_group = '<25 years old' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.age_group
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as under_25_FN_LP,

    d.sum_25_50_FN::float /(select sum(case when c.age_group = '25 - 50 years old' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.age_group
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as s25_50_FN_LP,

    d.sum_over_50_FN::float /(select sum(case when c.age_group = '> 50 years old' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.age_group
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as over_50_FN_LP,

    d.sum_not_known_FN::float /(select sum(case when c.age_group = 'not known' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.age_group
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as not_known_FN_LP


    from(

    select
    sum(case when c.age_group = 'not known' then 1 else 0 end) as sum_not_known_after_k,
    sum(case when c.age_group = '<25 years old' then 1 else 0 end) as sum_under_25_after_k,
    sum(case when c.age_group = '25 - 50 years old' then 1 else 0 end) as sum_25_50_after_k,
    sum(case when c.age_group = '> 50 years old' then 1 else 0 end) as sum_over_50_after_k,
    sum(case when c.age_group = 'not known' and label = 1 then 1 else 0 end) as sum_not_known_FN,
    sum(case when c.age_group = '<25 years old' and label = 1 then 1 else 0 end) as sum_under_25_FN,
    sum(case when c.age_group = '25 - 50 years old' and label = 1 then 1 else 0 end) as sum_25_50_FN,
    sum(case when c.age_group = '> 50 years old' and label = 1 then 1 else 0 end) as sum_over_50_FN

    from

    (select
    a.*,
    b.age_group
    from (
    select *, rank() over (order by score desc) from results.predictions
    where model = {0}
    order by score desc) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where rank > {1}) c

    ) d) sub)""".format(model, k))
    c.close()


def run_query_gender(k, model, environs):
    _, c = connect_cursor(environs)
    c.execute("""
    create view results.bmgk{1}model{0} as (
    select
    --fn_gs
    female_fn_gs::float/male_fn_gs::float as female_fn_gs_rate,
    non_binary_fn_gs::float/male_fn_gs::float as non_binary_fn_gs_rate,
    unknown_fn_gs::float/male_fn_gs::float as unknown_fn_gs_rate,

    --fn_pn
    female_fn_pn::float/male_fn_pn::float as female_fn_pn_rate,
    non_binary_fn_pn::float/male_fn_pn::float as non_binary_fn_pn_rate,
    unknown_fn_pn::float/male_fn_pn::float as unknown_fn_pn_rate,

    --fn_lp
    female_fn_lp::float/male_fn_lp::float as female_fn_lp_rate,
    non_binary_fn_lp::float/male_fn_lp::float as non_binary_fn_lp_rate,
    unknown_fn_lp::float/male_fn_lp::float as unknown_fn_lp_rate,

    --false negatives
    sum_female_fn::float/sum_male_fn::float as female_fn_rate,
    sum_non_binary_fn::float/sum_male_fn::float as non_binary_fn_rate,
    sum_unknown_fn::float/sum_male_fn::float as unknown_fn_rate,

    --after k
    sum_female_after_k::float/sum_male_after_k::float as female_after_k_rate,
    sum_non_binary_after_k::float/sum_male_after_k::float as non_binary_after_k_rate,
    sum_unknown_after_k::float/sum_male_after_k::float as unknown_after_k_rate


    FROM(
    select
    -- false negatives
    d.sum_female_FN,
    d.sum_male_FN,
    d.sum_non_binary_FN,
    d.sum_unknown_FN,

    --sum after k
    d.sum_female_after_k,
    d.sum_male_after_k,
    d.sum_non_binary_after_k,
    d.sum_unknown_after_k,

    --false negatives / group size from semantic.alerts_with_features
    d.sum_female_FN::float/(select count(e.gender)
    from (
    select a.score, b.gender from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and gender = 'female') e ) as female_FN_GS,

    d.sum_male_FN::float/(select count(e.gender)
    from (
    select a.score, b.gender from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and gender = 'male') e ) as male_FN_GS,

    d.sum_non_binary_FN::float/(select count(e.gender)
    from (
    select a.score, b.gender from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and gender = 'non-binary') e ) as non_binary_FN_GS,

    d.sum_unknown_FN::float/(select count(e.gender)
    from (
    select a.score, b.gender from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0} and gender = 'unknown') e ) as unknown_FN_GS,



    -- false negatives / predictived negatives
    d.sum_female_FN::float/d.sum_female_after_k::float as female_FN_PN,
    d.sum_male_FN::float / d.sum_male_after_k::float as male_FN_PN,
    d.sum_non_binary_FN::float/d.sum_non_binary_after_k::float as non_binary_FN_PN,
    d.sum_unknown_FN::float/d.sum_unknown_after_k::float as unknown_FN_PN,

    -- false negatives / total positive within fold (entire list - not broken by k)
    d.sum_female_FN::float /(select sum(case when c.gender = 'female' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.gender
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as female_FN_LP,

    d.sum_male_FN::float /(select sum(case when c.gender = 'male' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.gender
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as male_FN_LP,

    d.sum_non_binary_FN::float /(select sum(case when c.gender = 'non-binary' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.gender
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as non_binary_FN_LP,

    d.sum_unknown_FN::float /(select sum(case when c.gender = 'unknown' and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    b.gender
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as unknown_FN_LP


    from(

    select
    sum(case when c.gender = 'female' then 1 else 0 end) as sum_female_after_k,
    sum(case when c.gender = 'male' then 1 else 0 end) as sum_male_after_k,
    sum(case when c.gender = 'non-binary' then 1 else 0 end) as sum_non_binary_after_k,
    sum(case when c.gender = 'unknown' then 1 else 0 end) as sum_unknown_after_k,

    sum(case when c.gender = 'female' and label = 1 then 1 else 0 end) as sum_female_FN,
    sum(case when c.gender = 'male' and label = 1 then 1 else 0 end) as sum_male_FN,
    sum(case when c.gender = 'non-binary' and label = 1 then 1 else 0 end) as sum_non_binary_FN,
    sum(case when c.gender = 'unknown' and label = 1 then 1 else 0 end) as sum_unknown_FN

    from

    (select
    a.*,
    b.gender
    from (
    select *, rank() over (order by score desc) from results.predictions
    where model = {0}
    order by score desc) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where rank > {1}) c
    ) d) sub)""".format(model, k))
    c.close()


def run_query_london(k, model, environs):
    _, c = connect_cursor(environs)
    c.execute(""" create view results.bmlk{1}model{0} as (
    select
    --fn_gs
    non_london_fn_gs::float/london_fn_gs::float as non_london_fn_gs_rate,

    --fn_pn
    non_london_fn_pn::float/london_fn_pn::float as non_london_fn_pn_rate,


    --fn_lp
    non_london_fn_lp::float/london_fn_lp::float as non_london_fn_lp_rate,


    --false negatives
    sum_non_london_fn::float/sum_london_fn::float as non_london_fn_rate,


    --after k
    sum_non_london_after_k::float/sum_london_after_k::float as non_london_after_k_rate



    FROM (


    select
    -- false negatives
    d.sum_london_FN,
    d.sum_non_london_FN,


    --sum after k
    d.sum_london_after_k,
    d.sum_non_london_after_k,

    --false negatives / group size from semantic.alerts_with_features
    d.sum_london_FN::float/(select sum(case when region = 'london' then 1 else 0 end) from
    (select a.score, b.region from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0}) e ) as london_FN_GS,

    d.sum_non_london_FN::float/(select sum(case when region != 'london' then 1 else 0 end)
    from (
    select a.score, b.region from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0}) e ) as non_london_FN_GS,


    -- false negatives / predictived negatives
    d.sum_london_FN::float/d.sum_london_after_k::float as london_FN_PN,
    d.sum_non_london_FN::float / d.sum_non_london_after_k::float as non_london_FN_PN,


    -- false negatives / total positive within fold (entire list - not broken by k)
    d.sum_london_FN::float /(select sum(case when c.binary_london = 1 and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    case when region = 'london' then 1 else 0 end as binary_london
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as london_FN_LP,

    d.sum_non_london_FN::float /(select sum(case when c.binary_london = 0 and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    case when region = 'london' then 1 else 0 end as binary_london
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as non_london_FN_LP


    from(

    select
    sum(case when c.binary_london = 1 then 1 else 0 end) as sum_london_after_k,
    sum(case when c.binary_london = 0 then 1 else 0 end) as sum_non_london_after_k,

    sum(case when c.binary_london = 1 and label = 1 then 1 else 0 end) as sum_london_FN,
    sum(case when c.binary_london = 0 and label = 1 then 1 else 0 end) as sum_non_london_FN

    from

    (select
    a.*,
    b.region,
    case when b.region = 'london' then 1 else 0 end as binary_london
    from (
    select *, rank() over (order by score desc) from results.predictions
    where model = {0}
    order by score desc) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where rank > {1}) c

    ) d) sub)""".format(model, k))
    c.close()


def run_query_notifier(k, model, environs):
    _, c = connect_cursor(environs)
    c.execute(""" create view results.bmnk{1}model{0} as (
    --compared to member of the public (non self)

    select
    --fn_gs
    self_fn_gs::float/non_self_fn_gs::float as self_fn_gs_rate,
    --fn_pn
    self_fn_pn::float/non_self_fn_pn::float as self_fn_pn_rate,
    --fn_lp
    self_fn_lp::float/non_self_fn_lp::float as self_fn_lp_rate,
    --false negatives
    sum_self_fn::float/sum_non_self_fn::float as self_fn_rate,
    --after k
    sum_self_after_k::float/sum_non_self_after_k::float as self_after_k_rate

    FROM (
    select
    -- false negatives
    d.sum_self_FN,
    d.sum_non_self_FN,


    --sum after k
    d.sum_self_after_k,
    d.sum_non_self_after_k,

    --false negatives / group size from semantic.alerts_with_features
    d.sum_self_FN::float/(select sum(case when notifier_type = 'self' then 1 else 0 end) from
    (select a.score, b.notifier_type from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0}) e ) as self_FN_GS,

    d.sum_non_self_FN::float/(select sum(case when notifier_type != 'self' then 1 else 0 end)
    from (
    select a.score, b.notifier_type from results.predictions a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where model = {0}) e ) as non_self_FN_GS,


    -- false negatives / predictived negatives
    d.sum_self_FN::float/d.sum_self_after_k::float as self_FN_PN,
    d.sum_non_self_FN::float / d.sum_non_self_after_k::float as non_self_FN_PN,


    -- false negatives / total positive within fold (entire list - not broken by k)
    d.sum_self_FN::float /(select sum(case when c.binary_self = 1 and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    case when notifier_type = 'self' then 1 else 0 end as binary_self
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as self_FN_LP,

    d.sum_non_self_FN::float /(select sum(case when c.binary_self = 0 and label = 1 then 1 else 0 end) as test from
    (select
    a.*,
    case when notifier_type = 'self' then 1 else 0 end as binary_self
    from (
    select * from results.predictions
    where model = {0}) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert) c) as non_self_FN_LP


    from(

    select
    sum(case when c.binary_self = 1 then 1 else 0 end) as sum_self_after_k,
    sum(case when c.binary_self = 0 then 1 else 0 end) as sum_non_self_after_k,

    sum(case when c.binary_self = 1 and label = 1 then 1 else 0 end) as sum_self_FN,
    sum(case when c.binary_self = 0 and label = 1 then 1 else 0 end) as sum_non_self_FN

    from

    (select
    a.*,
    b.notifier_type,
    case when b.notifier_type = 'self' then 1 else 0 end as binary_self
    from (
    select *, rank() over (order by score desc) from results.predictions
    where model = {0}
    order by score desc) a
    left join
    semantic.alerts_with_features b on a.alert = b.alert
    where rank > {1}) c

    ) d) sub)""".format(model, k))
    c.close()
