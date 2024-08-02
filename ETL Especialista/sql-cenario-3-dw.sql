SELECT NK_PRODUTO_MOVIMENTO,
       NK_PRODUTO,
       NK_EMPRESA,
       NK_LOCAL_ESTOQUE,
       NK_NOTA_FISCAL,
       NK_OPERACAO,
       NK_PESSOA,
       VL_TOTAL_LIQUIDO,
       VL_QTD_PRODUTO,
       DT_MOVIMENTO
FROM
    DW_MANUAL.FAT_PRODUTO_MOVIMENTO where DT_MOVIMENTO between current_date and current_date - interval '2 months'