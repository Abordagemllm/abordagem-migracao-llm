SELECT
    (ea.idplanilha::varchar||ea.numsequencia::varchar)::bigint AS NK_PRODUTO_MOVIMENTO,
    idproduto AS NK_PRODUTO,
    ea.idempresa AS NK_EMPRESA,
    idlocalestoque AS NK_LOCAL_ESTOQUE,
    n.idplanilha AS NK_NOTA_FISCAL,
    idoperacao AS NK_OPERACAO,
    n.idclifor AS NK_PESSOA,
    valtotliquido AS VL_TOTAL_LIQUIDO,
    qtdproduto AS VL_QTD_PRODUTO,
    n.dtmovimento::date AS DT_MOVIMENTO
FROM
    public.estoque_analitico as ea
    join public.nota as n on (n.idplanilha = ea.idplanilha)
where ea.dtmovimento between current_date and current_date - interval '2 months' AND numsequencia > 0