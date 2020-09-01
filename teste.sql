set SEARCH_PATH to utilizador_0000000011,public;
SELECT
       'Atendimento'   AS entidade,
       A.ID                                             id_entidade,
       A.ID                                             id_atendimento,
       C.ID                                             id_cliente,
       U.ID                                             ID_USUARIO,
       PF.CPF,
       CASE WHEN PD.ID_PESEND IS NULL
            THEN NULL
            ELSE COALESCE(PD.LOGRADOURO,'') ||  ', ' || COALESCE(PD.NUMERO,'') || ' ' || COALESCE(PD.BAIRRO,'') || CASE WHEN COALESCE(PD.CEP,'') <> '' THEN ', CEP ' || PD.CEP ELSE '' END || CASE WHEN C.ID IS NOT NULL THEN ', ' || COALESCE(PD.CIDADE_NOME,'') || ' - ' || COALESCE(PD.UF,'') ELSE '' END
        END ENDERECO,
       COALESCE(U.AVATAR, 'resources/core/img/boy.png') AVATAR,
       U.LOGIN,
       A.ID_TIPO,
       A.PROTOCOLO,
       A.OBSERVACAO,
       A.DESCRICAO,
       A.ID_STATUS,
       TA.CONSTANTE                                     tipo,
       TA.ICONE                                         icone_tipo,
       TA.COR                                           cor_tipo,
       AE_PROD_RECLAM.DESC_PRODUTO      DESC_PRODUTO,
       AE_PROD_RECLAM.PRODUTO_DESCRICAO_FULL PRODUTO_RECLAMADO_DESCRICAO_FULL,
       AE_PROD_RECLAM.VALOR_CUSTO       VALOR_CUSTO,
       AE_PROD_RECLAM.VALOR_PRECO_VENDA VALOR_PRECO_VENDA,
       AE_PROD_RECLAM.CLASSIFICACAO     CLASSIFICACAO,
       AE_PROD_RECLAM.RESP_TEMPO_COMPRA TEMPO_COMPRA,
       AE_PROD_RECLAM.FABRICA_NOME, AE_PROD_RECLAM.FABRICA_CNPJ,

       AE_PROD_RECEB.PRODUTO_DESCRICAO_FULL PRODUTO_RECEBIDO_DESCRICAO_FULL,
       AE_PROD_RECEB.RESP_DATA_ANALISE PROD_RECEB_DT_ANALISE,
       AE_PROD_RECEB.TIPO_CLASSIFICACAO PROD_RECEB_CLASSIFICACAO,

       AE_PROD_ENV.DESC_PRODUTO      DESC_PRODUTO_ENVIADO,
       AE_PROD_ENV.PRODUTO_DESCRICAO_FULL PRODUTO_ENVIADO_DESCRICAO_FULL,

       COALESCE(AE_FABRICA.FAB_FANTASIA, AE_PROD_ENV.FABRICA_NOME) PROD_ENV_FAB_NOME,
       COALESCE(AE_FABRICA.FAB_CNPJ, AE_PROD_ENV.FABRICA_CNPJ) PROD_ENV_FAB_CNPJ,

       AE_PROD_ENV.TIPO_CLASSIFICACAO PRODUTO_ENVIADO_CLASSIFICACAO,
       AE_PROD_ENV.TIPO_CLASSIFICACAO PROD_ENVIADO_CLASSIFICACAO_FILTRO,
       TA.desc_full                                     desc_tipo,
       SA.CONSTANTE                                     status,
       OA.DESCRICAO
       || CASE
            WHEN OA.CONSTANTE = 'EMAIL'
                 AND EMM.ID_ATENDIMENTO IS NOT NULL THEN ' ('
                                                         || EMM.EMAIL_ENDERECO
                                                         || ')'
            WHEN OA.CONSTANTE = 'RECLAME_AQUI'
                 AND RAET.ID_ATENDIMENTO IS NOT NULL THEN ' ('
                                                          || RAET.RA_EMPRESA
                                                          || ')'
            ELSE ''
          END                                           ORIGEM,
       OA.CONSTANTE ORIGEM_CONSTANTE,
       SA.TIPO_TEMPO_SLA,
       SA.TEMPO_SLA,
       SA.ORDEM                                         ordem_status,
       SA.COR                                           cor_status,
       SA.ID                                            id_status,
       SA.DESCRICAO                                     desc_status,
       P.NOME_REDUZ,
       COALESCE(P.NOME, 'NÃO IDENTIFICADO')            NOME,
       A.DH_AGENDA,
       A.DH_REGISTRO,
       AE_PROD_RECLAM.DH_LANCAMENTO DH_LANCAMENTO,
       REEMB.BANCO   BANCO,
       REEMB.AGENCIA AGENCIA,
       REEMB.CONTA   CONTA,
       REEMB.VALOR   VALOR_REEMBOLSO,
       REEMB.MOTIVO  MOTIVO_REEMBOLSO,
       ETIQ_ENV_PROD.PROD_ETIQ_ENVIO,
       ETIQ_ENV_PROD.PROD_DH_ENVIO,
       ETIQ_REVERSA.PROD_COD_RASTREIO_REVERSO,
       AI_EXT.DH_REGISTRO DH_ULT_INT_EXTERNA,
       COALESCE(LEC_STATUS_ATUAL.DH_ENTRADA_STATUS,A.DH_REGISTRO) DH_STATUS
  FROM MKR_ATENDIMENTO A

 INNER JOIN MKR_STATUS_WORKFLOW    SA ON ( SA.ID = A.ID_STATUS )
 INNER JOIN MKR_ORIGEM_ATENDIMENTO OA ON ( OA.ID = A.ID_ORIGEM )

  LEFT JOIN MKR_CLIENTE C ON ( C.ID = A.ID_CLIENTE )
  LEFT JOIN MKR_PESSOA  P ON ( P.ID = C.ID_PESSOA )
  LEFT JOIN MKR_PESSOA_FISICA PF ON (PF.ID_PESSOA = P.ID)

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT PS.ID ID_PESEND, CD.DESCRICAO CIDADE_NOME, CE.UF, PS.*
                                      , ROW_NUMBER() OVER (PARTITION BY PS.ID_PESSOA ORDER BY PS.ID_PESSOA, PS.ID) RN_PESEND
                                 FROM MKR_PESSOA_ENDERECO PS, MKR_CIDADE CD, MKR_ESTADO CE
                                WHERE 1 = 1
                                  AND PS.ID_PESSOA = P.ID
                                  AND CE.ID = CD.ID_ESTADO
                                  AND CD.ID = PS.ID_CIDADE
                                  AND PS.ATIVO = true
                                  AND 1 = 1 ) TBL
                       WHERE TBL.RN_PESEND = 1 ) PD ON 1 = 1

  LEFT JOIN MKR_ATENDENTE        AT ON AT.ID = A.ID_ATENDENTE
  LEFT JOIN MKR_USUARIO          U  ON U.ID  = AT.ID_USUARIO
  left join ( WITH RECURSIVE ta_sup AS (
                                       SELECT id, descricao, constante, icone, cor, 1 as level, null desc_full
                                       FROM   mkr_tipo_atendimento
                                       WHERE  id_superior is null

                                       UNION  ALL

                                       SELECT ta.id, ta.descricao, ta.constante, ta.icone, ta.cor, ta_sup.level +1, ta_sup.descricao || ' -> ' || ta.descricao desc_full
                                       FROM   ta_sup
                                       JOIN   mkr_tipo_atendimento ta ON ta.id_superior = ta_sup.id
                                       )
             select id, descricao, constante, icone, cor, level, desc_full
               from ta_sup ) ta on ta.id = a.id_tipo

  LEFT JOIN ( SELECT EMM.ID_ATENDIMENTO,
                     EM.NOME     EMAIL_NOME,
                     EM.ENDERECO EMAIL_ENDERECO
                FROM MKR_EMAIL_MONIT_MENSAGEM EMM

               INNER JOIN MKR_EMAIL_MONITORAMENTO EM ON EM.ID = EMM.ID_EMAIL_MONITORAMENTO

               WHERE 1 = 1
                 AND EMM.ID_ATENDIMENTO_INTERACAO IS NULL
                 AND 1 = 1 ) EMM ON EMM.ID_ATENDIMENTO = A.ID

  LEFT JOIN ( SELECT RAET.ID_ATENDIMENTO,
                     RAE.COMPANY_SHORTNAME RA_EMPRESA
                FROM MKR_RECLAME_AQUI_EMP_TICKET RAET

               INNER JOIN MKR_RECLAME_AQUI_EMPRESA RAE ON RAE.ID = RAET.ID_RECLAME_AQUI_EMPRESA

               WHERE 1 = 1
                 AND 1 = 1 ) RAET ON RAET.ID_ATENDIMENTO = A.ID

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT AE.ID ID_ATENDIMENTO_ENTIDADE,
                                      AE.ID_ATENDIMENTO,
                                      TC.NOME               CLASSIFICACAO,
                                      AEFCR_TEMPO_COMPRA.RESP_TEMPO_COMPRA,
                                      P.ID                  ID_HUB_PRODUTO,
                                      P.DESCRICAO           DESC_PRODUTO,
                                      P.DH_LANCAMENTO       DH_LANCAMENTO,
                                      G.GRADE               PRODUTO_GRADE,
                                      P.CODIGO_REFERENCIA || ' - ' || P.DESCRICAO || ' ' || G.GRADE PRODUTO_DESCRICAO_FULL,
                                      TPI.VALOR_CUSTO       VALOR_CUSTO,
                                      TPI.VALOR_PRECO_VENDA VALOR_PRECO_VENDA,
                                      P_FORN.NOME_REDUZ FABRICA_NOME, PJ_FORN.CNPJ FABRICA_CNPJ
                                      , ROW_NUMBER() OVER (PARTITION BY AE.ID_ATENDIMENTO ORDER BY AE.ID_ATENDIMENTO, AE.ID) RN_ATEND_ENT
                                 FROM MKR_ATENDIMENTO_ENTIDADE AE

                                INNER JOIN MKR_ATENDIMENTO_VINCULO AV ON AV.ID = AE.ID_VINCULO

                                INNER JOIN MKR_HUB_PRODUTO_SKU     PS ON PS.ID = AE.ID_ENTIDADE
                                INNER JOIN MKR_HUB_PRODUTO         P  ON P.ID  = PS.ID_HUB_PRODUTO

                                 LEFT JOIN LATERAL ( SELECT SGI.ID_HUB_PRODUTO_SKU, STRING_AGG('[' || G.CODIGO || ' ' || (CASE WHEN G.TIPO = 'COR' THEN GI.CODIGO || ' - ' ELSE '' END) || GI.LEGENDA || ']',' ' ORDER BY G.CODIGO) GRADE
                                                       FROM MKR_HUB_PRODUTO_SKU_GRADE_ITEM SGI, MKR_HUB_GRADE_ITEM GI, MKR_HUB_GRADE G
                                                      WHERE 1 = 1
                                                        AND GI.ID = SGI.ID_HUB_GRADE_ITEM
                                                        AND G.ID  = GI.ID_HUB_GRADE
                                                        AND SGI.ID_HUB_PRODUTO_SKU = PS.ID
                                                        AND 1 = 1
                                                     GROUP BY SGI.ID_HUB_PRODUTO_SKU ) G ON 1 = 1

                                 LEFT JOIN MKR_HUB_TABELA_PRECO_ITEM TPI ON TPI.ID_HUB_PRODUTO = P.ID

                                 LEFT JOIN MKR_TIPO_CLASSIFICACAO  TC ON TC.ID = AE.ID_TIPO_CLASSIFICACAO

                                 LEFT JOIN MKR_PESSOA          P_FORN  ON P_FORN.ID = P.ID_FORNECEDOR
                                 LEFT JOIN MKR_PESSOA_JURIDICA PJ_FORN ON PJ_FORN.ID_PESSOA = P_FORN.ID

                                 LEFT JOIN ( SELECT AEFCR.ID_ATENDIMENTO_ENTIDADE, AEFCR.RESPOSTA RESP_TEMPO_COMPRA
                                               FROM MKR_ATEND_ENT_FORM_CLASS_RESP AEFCR
                                              WHERE 1 = 1
                                                AND EXISTS ( SELECT 1
                                                               FROM MKR_FORMULARIO_CLASSIFIC_QUES FCQ
                                                              WHERE FCQ.ID = AEFCR.ID_FORMULARIO_CLASSIFICACAO_QUES
                                                                AND FCQ.CONSTANTE = 'TEMPO_COMPRA_DIAS' )
                                                AND 1 = 1 ) AEFCR_TEMPO_COMPRA ON AEFCR_TEMPO_COMPRA.ID_ATENDIMENTO_ENTIDADE = AE.ID

                                WHERE 1 = 1
                                  AND AV.ENTIDADE       = 'HubProdutoSKU'
                                  AND AV.CONSTANTE      = 'PRODUTO_RECLAMADO'
                                  AND AE.ID_ATENDIMENTO = A.ID
                                  AND 1 = 1 ) TBL
                       WHERE TBL.RN_ATEND_ENT = 1 ) AE_PROD_RECLAM ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT AE.ID ID_ATENDIMENTO_ENTIDADE,
                                      AE.ID_ATENDIMENTO,
                                      TC.NOME               CLASSIFICACAO,
                                      AEFCR_DATA_ANALISE.RESP_DATA_ANALISE,
                                      P.ID                  ID_HUB_PRODUTO,
                                      P.DESCRICAO           DESC_PRODUTO,
                                      P.DH_LANCAMENTO       DH_LANCAMENTO,
                                      G.GRADE               PRODUTO_GRADE,
                                      P.CODIGO_REFERENCIA || ' - ' || P.DESCRICAO || ' ' || G.GRADE PRODUTO_DESCRICAO_FULL,
                                      TPI.VALOR_CUSTO       VALOR_CUSTO,
                                      TPI.VALOR_PRECO_VENDA VALOR_PRECO_VENDA,
                                      TC.NOME TIPO_CLASSIFICACAO
                                      , ROW_NUMBER() OVER (PARTITION BY AE.ID_ATENDIMENTO ORDER BY AE.ID_ATENDIMENTO, AE.ID) RN_ATEND_ENT
                                 FROM MKR_ATENDIMENTO_ENTIDADE AE

                                INNER JOIN MKR_ATENDIMENTO_VINCULO AV ON AV.ID = AE.ID_VINCULO

                                INNER JOIN MKR_HUB_PRODUTO_SKU     PS ON PS.ID = AE.ID_ENTIDADE
                                INNER JOIN MKR_HUB_PRODUTO         P  ON P.ID  = PS.ID_HUB_PRODUTO

                                 LEFT JOIN LATERAL ( SELECT SGI.ID_HUB_PRODUTO_SKU, STRING_AGG('[' || G.CODIGO || ' ' || (CASE WHEN G.TIPO = 'COR' THEN GI.CODIGO || ' - ' ELSE '' END) || GI.LEGENDA || ']',' ' ORDER BY G.CODIGO) GRADE
                                                       FROM MKR_HUB_PRODUTO_SKU_GRADE_ITEM SGI, MKR_HUB_GRADE_ITEM GI, MKR_HUB_GRADE G
                                                      WHERE 1 = 1
                                                        AND GI.ID = SGI.ID_HUB_GRADE_ITEM
                                                        AND G.ID  = GI.ID_HUB_GRADE
                                                        AND SGI.ID_HUB_PRODUTO_SKU = PS.ID
                                                        AND 1 = 1
                                                     GROUP BY SGI.ID_HUB_PRODUTO_SKU ) G ON 1 = 1

                                 LEFT JOIN MKR_HUB_TABELA_PRECO_ITEM TPI ON TPI.ID_HUB_PRODUTO = P.ID

                                 LEFT JOIN MKR_TIPO_CLASSIFICACAO  TC ON TC.ID = AE.ID_TIPO_CLASSIFICACAO

                                 LEFT JOIN ( SELECT AEFCR.ID_ATENDIMENTO_ENTIDADE, AEFCR.RESPOSTA RESP_DATA_ANALISE
                                               FROM MKR_ATEND_ENT_FORM_CLASS_RESP AEFCR
                                              WHERE 1 = 1
                                                AND EXISTS ( SELECT 1
                                                               FROM MKR_FORMULARIO_CLASSIFIC_QUES FCQ
                                                              WHERE FCQ.ID = AEFCR.ID_FORMULARIO_CLASSIFICACAO_QUES
                                                                AND FCQ.CONSTANTE = 'DATA_ANALISE' )
                                                AND 1 = 1 ) AEFCR_DATA_ANALISE ON AEFCR_DATA_ANALISE.ID_ATENDIMENTO_ENTIDADE = AE.ID

                                WHERE 1 = 1
                                  AND AV.ENTIDADE       = 'HubProdutoSKU'
                                  AND AV.CONSTANTE      = 'PRODUTO_RECEBIDO'
                                  AND AE.ID_ATENDIMENTO = A.ID
                                  AND 1 = 1 ) TBL
                       WHERE TBL.RN_ATEND_ENT = 1 ) AE_PROD_RECEB ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT AE.ID_ATENDIMENTO,
                                      P.ID                  ID_HUB_PRODUTO,
                                      P.DESCRICAO           DESC_PRODUTO,
                                      P.DH_LANCAMENTO       DH_LANCAMENTO,
                                      G.GRADE               PRODUTO_GRADE,
                                      P.CODIGO_REFERENCIA || ' - ' || P.DESCRICAO || ' ' || G.GRADE PRODUTO_DESCRICAO_FULL,
                                      P_FORN.NOME_REDUZ FABRICA_NOME, PJ_FORN.CNPJ FABRICA_CNPJ,
                                      TC.NOME TIPO_CLASSIFICACAO
                                      , ROW_NUMBER() OVER (PARTITION BY AE.ID_ATENDIMENTO ORDER BY AE.ID_ATENDIMENTO, AE.ID) RN_ATEND_ENT
                                 FROM MKR_ATENDIMENTO_ENTIDADE AE

                                INNER JOIN MKR_ATENDIMENTO_VINCULO AV ON AV.ID = AE.ID_VINCULO

                                INNER JOIN MKR_HUB_PRODUTO_SKU     PS ON PS.ID = AE.ID_ENTIDADE
                                INNER JOIN MKR_HUB_PRODUTO         P  ON P.ID  = PS.ID_HUB_PRODUTO

                                 LEFT JOIN MKR_TIPO_CLASSIFICACAO  TC ON TC.ID = AE.ID_TIPO_CLASSIFICACAO

                                 LEFT JOIN MKR_PESSOA          P_FORN  ON P_FORN.ID = P.ID_FORNECEDOR
                                 LEFT JOIN MKR_PESSOA_JURIDICA PJ_FORN ON PJ_FORN.ID_PESSOA = P_FORN.ID

                                 LEFT JOIN LATERAL ( SELECT SGI.ID_HUB_PRODUTO_SKU, STRING_AGG('[' || G.CODIGO || ' ' || (CASE WHEN G.TIPO = 'COR' THEN GI.CODIGO || ' - ' ELSE '' END) || GI.LEGENDA || ']',' ' ORDER BY G.CODIGO) GRADE
                                                       FROM MKR_HUB_PRODUTO_SKU_GRADE_ITEM SGI, MKR_HUB_GRADE_ITEM GI, MKR_HUB_GRADE G
                                                      WHERE 1 = 1
                                                        AND GI.ID = SGI.ID_HUB_GRADE_ITEM
                                                        AND G.ID  = GI.ID_HUB_GRADE
                                                        AND SGI.ID_HUB_PRODUTO_SKU = PS.ID
                                                        AND 1 = 1
                                                      GROUP BY SGI.ID_HUB_PRODUTO_SKU ) G ON 1 = 1

                                WHERE 1 = 1
                                  AND AV.ENTIDADE       = 'HubProdutoSKU'
                                  AND AV.CONSTANTE      = 'PRODUTO_ENVIADO'
                                  AND AE.ID_ATENDIMENTO = A.ID
                                  AND 1 = 1 ) TBL
                       WHERE 1 = 1
                         AND 1 = 1 ) AE_PROD_ENV ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT AE.ID_ATENDIMENTO,
                                      P.ID         FAB_ID,
                                      P.NOME       FAB_NOME,
                                      P.NOME_REDUZ FAB_FANTASIA,
                                      PJ.CNPJ      FAB_CNPJ
                                      , ROW_NUMBER() OVER (PARTITION BY AE.ID_ATENDIMENTO ORDER BY AE.ID_ATENDIMENTO, AE.ID) RN_ATEND_ENT
                                 FROM MKR_ATENDIMENTO_ENTIDADE AE

                                INNER JOIN MKR_ATENDIMENTO_VINCULO AV ON AV.ID = AE.ID_VINCULO

                                INNER JOIN MKR_PESSOA          P  ON P.ID = AE.ID_ENTIDADE
                                INNER JOIN MKR_PESSOA_JURIDICA PJ ON PJ.ID_PESSOA = P.ID

                                WHERE 1 = 1
                                  AND AV.ENTIDADE       = 'Pessoa'
                                  AND AV.CONSTANTE      = 'FABRICA'
                                  AND AE.ID_ATENDIMENTO = A.ID
                                  AND 1 = 1 ) TBL
                       WHERE 1 = 1
                         AND TBL.RN_ATEND_ENT = 1
                         AND 1 = 1 ) AE_FABRICA ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT AR.ID_ATENDIMENTO, AR.VALOR, AR.OBSERVACAO, MR.DESCRICAO MOTIVO, PDB.AGENCIA, PDB.CONTA, B.DESCRICAO BANCO
                                      , ROW_NUMBER() OVER (PARTITION BY AR.ID_ATENDIMENTO ORDER BY AR.ID_ATENDIMENTO, AR.ID) RN_ATEND_REEMB
                                 FROM MKR_ATENDIMENTO_RESSARCIMENTO AR

                                INNER JOIN MKR_PESSOA_DADOS_BANCARIOS PDB ON PDB.ID = AR.ID_DADOS_BANCARIOS
                                INNER JOIN MKR_BANCO                  B   ON B.ID   = PDB.ID_BANCO
                                INNER JOIN MKR_MOTIVO_REEMBOLSO       MR  ON MR.ID  = AR.ID_MOTIVO

                                WHERE 1 = 1
                                  AND AR.ID_ATENDIMENTO = A.ID
                                  AND 1 = 1 ) TBL
                       WHERE TBL.RN_ATEND_REEMB = 1 ) REEMB ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT ID_ENTIDADE, CE.CODIGO_RASTREIO PROD_ETIQ_ENVIO, CE.DH_REGISTRO PROD_DH_ENVIO
                                      , ROW_NUMBER() OVER (PARTITION BY CEO.ID_ENTIDADE ORDER BY CEO.ID_ENTIDADE, CEO.ID) RN_ETIQUETA
                                 FROM MKR_CORREIO_ETIQUETA_ORIGEM CEO

                                INNER JOIN MKR_CORREIO_ETIQUETA CE ON CE.ID = CEO.ID_ETIQUETA

                                WHERE 1 = 1
                                  AND CEO.CLASSE_ENTIDADE = 'Atendimento'
                                  AND CEO.ID_ENTIDADE     = A.ID
                                  AND NOT EXISTS ( SELECT 1 FROM MKR_CORREIO_LOGISTICA_REVERSA_RETORNO CLRR WHERE CLRR.ID_ETIQUETA_RETORNO = CE.ID )
                                  AND 1 = 1 ) TBL
                       WHERE TBL.RN_ETIQUETA = 1 ) ETIQ_ENV_PROD ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT CLRO.ID_ENTIDADE, CE.CODIGO_RASTREIO PROD_COD_RASTREIO_REVERSO
                                      , ROW_NUMBER() OVER (PARTITION BY CLRO.ID_ENTIDADE ORDER BY CLRO.ID_ENTIDADE, CLRO.ID) RN_ETIQUETA
                                 FROM MKR_CORREIO_LOGISTICA_REVERSA_ORIGEM CLRO

                                INNER JOIN MKR_CORREIO_LOGISTICA_REVERSA_RETORNO CLRR ON CLRR.ID = CLRO.ID_LOGISTICA_REVERSA_RETORNO
                                INNER JOIN MKR_CORREIO_ETIQUETA CE ON CE.ID = CLRR.ID_ETIQUETA_RETORNO

                                WHERE 1 = 1
                                  AND CLRO.CLASSE_ENTIDADE = 'Atendimento'
                                  AND CLRO.ID_ENTIDADE = A.ID
                                  AND 1 = 1 ) TBL
                       WHERE TBL.RN_ETIQUETA = 1 ) ETIQ_REVERSA ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                       FROM ( SELECT AI.ID_ATENDIMENTO, AI.DH_REGISTRO
                                     , ROW_NUMBER() OVER (PARTITION BY AI.ID_ATENDIMENTO ORDER BY AI.DH_REGISTRO DESC) RN_INT_EXTERNA
                                FROM MKR_ATENDIMENTO_INTERACAO AI

                               WHERE 1 = 1
                                 AND AI.ID_ATENDIMENTO = A.ID
                                 AND NOT EXISTS ( SELECT 1 FROM MKR_ATENDENTE ATEND WHERE ATEND.ID_USUARIO = AI.ID_USUARIO )
                                 AND AI.DH_REGISTRO > (SELECT MAX(AI_INT.DH_REGISTRO) FROM MKR_ATENDIMENTO_INTERACAO AI_INT WHERE AI_INT.ID_ATENDIMENTO = AI.ID_ATENDIMENTO AND EXISTS ( SELECT 1 FROM MKR_ATENDENTE ATEND WHERE ATEND.ID_USUARIO = AI_INT.ID_USUARIO ))
                                 AND 1 = 1 ) TBL
                      WHERE TBL.RN_INT_EXTERNA = 1 ) AI_EXT ON 1 = 1

  LEFT JOIN LATERAL ( SELECT TBL.*
                        FROM ( SELECT LE.ID_ENTIDADE, LEC.VALOR_NOVO, LE.DH_REGISTRO DH_ENTRADA_STATUS
                                       , ROW_NUMBER() OVER (PARTITION BY LE.ID_ENTIDADE, LEC.VALOR_NOVO ORDER BY LE.ID_ENTIDADE, LEC.VALOR_NOVO, LE.DH_REGISTRO DESC) RN_LOG_ENTIDADE
                                 FROM MKR_LOG_ENTIDADE LE

                                INNER JOIN MKR_LOG_ENTIDADE_CAMPO LEC ON LEC.ID_LOG_ENTIDADE = LE.ID
                                                                     AND LEC.CAMPO = 'status'

                                WHERE 1 = 1
                                  AND LE.CLASSE      = 'Atendimento'
                                  AND LE.ID_ENTIDADE = A.ID
                                  AND LE.TABELA      = 'MKR_ATENDIMENTO'
                                  AND LEC.VALOR_NOVO = A.ID_STATUS::TEXT
                                  AND 1 = 1 ) TBL
                       WHERE TBL.RN_LOG_ENTIDADE = 1 ) LEC_STATUS_ATUAL ON 1 = 1

 WHERE 1 = 1
   AND SA.ARQUIVO_MORTO = false
   AND ( A.ID_STATUS  = 1  OR 0 = 2  )
 ORDER BY COALESCE(A.DH_AGENDA, A.DH_REGISTRO);
