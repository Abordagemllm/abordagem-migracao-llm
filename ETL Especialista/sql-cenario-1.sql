SELECT idplanilha as nk_nota_fiscal,
       numnota as nr_nota_fiscal,
       serienota as ds_serie_nota_fiscal
FROM
    public.nota
where
    dtmovimento between '2023-01-01' and '2023-12-31'