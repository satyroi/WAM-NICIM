----------INTERF_PARTI(ITEM_MASTER)----------
select t1.part,t1.vendor,t1.quodate,t1.descshort as lt,ltrim(rtrim(substring(description,5,len(description)))) as eoq,left(description,3) as pre into #pricelist from
u8m_wx..ven10110 t1,
(select max(t1.vendor)vendor,t1.part,t1.quodate from u8m_wx..ven10110 t1,
(select max(isnull(convert(int,left(description,3)),0))per,max(quodate)quodate,part from u8m_wx..ven10110 
group by part)t2
where t1.part=t2.part and isnull(convert(int,left(t1.description,3)),0)=t2.per and t1.quodate = t2.quodate and t2.per!=0
group by t1.part,t1.quodate)t2
where t1.vendor=t2.vendor and t1.part=t2.part;
select IDENTITY(INT,1,1) as seq,
t1.part as pa_code,
nicim.dbo.RemoveCharCN(t1.userdef1) as pa_desc,
t1.itemtype as pa_fami,
case t1.bypass 
  when 'F' then '4' 
  else 
     case t1.type 
       when '0' then '1'
       when '2' then '2'
       when '4' then '3'
       when '6' then '2'
     end
end as pa_tipo,
t3.glp as pa_pglp,
isnull(t2.eoq,t3.eoq) as pa_qmin,
t1.safqty as pa_scmi,
nicim.dbo.RemoveCharCN(left(t1.uom,2)) as pa_umte,
nicim.dbo.RemoveCharCN(left(t1.pounit,2)) as pa_umco,
t1.pofact as pa_fatt,
t1.mutqty as pa_ldtp,
t2.vendor as pa_forn,
t3.production_line as pa_at01,
t1.userdef5 as pa_at02,
t2.quodate as pa_ldt1,
t3.marking_attribute as pa_marc,
t3.safety_stock_k as pa_fatk,
t3.kanban as pa_fkan,
t3.safety_stock as pa_fsco,
t3.reorder_point as pa_frio,
t3.planning as pa_fpla,
t3.jit as pa_fjit,
t3.coverage_days as pa_gcop,
t1.maxqty as pa_lnr1,
case t1.status 
  when '1' then '1' 
  else 
     '4'
end as pa_stat,
t1.abcctl as pa_free,
t1.reorder as pa_rior,
t3.days_in_advance as pa_gant,
t2.pre as pa_lnr2
into #t
from u8m_wx..inv10100 t1,#pricelist t2,openrowset('SQLOLEDB','172.17.0.5\u8m';'pcsamin';'Mappetsci0',nicim.dbo.masterdata) t3
where t1.part*=t2.part and t1.part*=t3.part;
truncate table INTERF_PARTI;
insert into INTERF_PARTI(pa_serial,pa_code,pa_desc,pa_fami,pa_tipo,pa_pglp,pa_qmin,pa_scmi,pa_umte,pa_umco,pa_fatt,pa_ldtp,pa_forn,pa_at01,pa_at02,pa_ldt1,pa_marc,pa_fatk,pa_fkan,pa_fsco,pa_frio,pa_fpla,pa_fjit,pa_gcop,pa_lnr1,pa_stat,pa_free,pa_rior,pa_gant,pa_lnr2) 
select * from #t;
drop table #pricelist;
drop table #t;

----------INTERF_DIBA(BOM)----------
select IDENTITY(INT,1,1) as seq,
parent as db_copd,
component as db_cofi,
qty as db_qtau
into #t
from u8m_wx..bom10101
where rtrim(component)!='';
truncate table INTERF_DIBA;
insert into INTERF_DIBA(db_serial,db_copd,db_cofi,db_qtau) 
select * from #t;
drop table #t;

----------INTERF_OCTE(HEADER_CUST_ORD)----------
select IDENTITY(INT,1,1) as seq,
t.ot_code,
t.ot_clie,
t.ot_decl
into #t from
(select t1.docno as ot_code,
'T'+t1.cust as ot_clie,
nicim.dbo.RemoveCharCN(t2.userdef1) as ot_decl
from u8m_tr..ord20110 t1,u8m_tr..cst10100 t2
where t1.cust=t2.cust and (t1.status='OP' or t1.status='SH')
union all
select t1.docno as ot_code,
'W'+t1.cust as ot_clie,
nicim.dbo.RemoveCharCN(t2.userdef1) as ot_decl
from u8m_wx..ord20110 t1,u8m_wx..cst10100 t2
where t1.cust=t2.cust and (t1.status='OP' or t1.status='SH'))t;
truncate table INTERF_OCTE;
insert into INTERF_OCTE(ot_serial,ot_code,ot_clie,ot_decl) 
select * from #t;
drop table #t;

----------INTERF_OCDE(DETAIL_CUST_ORD)----------
select IDENTITY(INT,1,1) as seq,
t.oc_code,
t.oc_riga,
t.oc_item,
t.oc_qtor,
t.oc_qtev,
t.oc_umqt,
t.oc_dtri,
t.oc_dtco,
t.oc_cu02
into #t from 
(select t1.docno as oc_code,
t1.docseq as oc_riga,
t1.part as oc_item,
t1.txqty as oc_qtor,
t1.shpqty as oc_qtev,
nicim.dbo.RemoveCharCN(left(t1.uom,2)) as oc_umqt,
t1.plandate as oc_dtri,
t1.stdate as oc_dtco,
CONVERT(varchar(100),t2.userdef4,111) as oc_cu02
from u8m_tr..ord20111 t1,u8m_tr..ord20110 t2
where t1.docno=t2.docno and (t1.status='OP' or t1.status='SH')
union all
select t1.docno as oc_code,
t1.docseq as oc_riga,
t1.part as oc_item,
t1.txqty as oc_qtor,
t1.shpqty as oc_qtev,
nicim.dbo.RemoveCharCN(left(t1.uom,2)) as oc_umqt,
t1.plandate as oc_dtri,
t1.stdate as oc_dtco,
CONVERT(varchar(100),t2.userdef4,111) as oc_cu02
from u8m_wx..ord20111 t1,u8m_wx..ord20110 t2
where t1.docno=t2.docno and (t1.status='OP' or t1.status='SH'))t;
truncate table INTERF_OCDE;
insert into INTERF_OCDE(oc_serial,oc_code,oc_riga,oc_item,oc_qtor,oc_qtev,oc_umqt,oc_dtri,oc_dtco,oc_cu02) 
select * from #t;
drop table #t;

----------INTERF_OFTE(HEADER_SUPP_ORD)----------
select IDENTITY(INT,1,1) as seq,
t.of_code,
t.of_forn,
t.of_defo
into #t from
(select t1.pono as of_code,
t1.vendor as of_forn,
nicim.dbo.RemoveCharCN(t2.userdef1) as of_defo
from u8m_wx..pur10100 t1,u8m_wx..ven10100 t2
where t1.vendor = t2.vendor and (t1.status='OP' or t1.status='IN' or t1.status='NA')
union all
select t1.pono as of_code,
t1.vendor as of_forn,
nicim.dbo.RemoveCharCN(t2.userdef1) as of_defo
from u8m_tr..pur10100 t1,u8m_tr..ven10100 t2
where t1.vendor = t2.vendor and (t1.status='OP' or t1.status='IN' or t1.status='NA'))t;
truncate table INTERF_OFTE;
insert into INTERF_OFTE(of_serial,of_code,of_forn,of_defo) 
select * from #t;
drop table #t;

----------INTERF_OFDE(DETAIL_SUPP_ORD)----------
select IDENTITY(INT,1,1) as seq,
t.od_code,
t.od_riga,
t.od_item,
t.od_qtor,
t.od_qtev,
t.od_umqt,
t.od_dtor,
t.od_dtri,
t.od_dtco
into #t from
(select
pono as od_code,
poseq as od_riga,
part as od_item,
qty as od_qtor,
acpqty as od_qtev,
nicim.dbo.RemoveCharCN(left(unit,2)) as od_umqt,
createdate as od_dtor,
precdate as od_dtri,
pacpdate as od_dtco
from u8m_wx..pur10110
where status='OP' or status='IN' or status='NA'
union all
select
pono as od_code,
poseq as od_riga,
nicim.dbo.RemoveCharCN(part) as od_item,
qty as od_qtor,
acpqty as od_qtev,
nicim.dbo.RemoveCharCN(left(unit,2)) as od_umqt,
createdate as od_dtor,
precdate as od_dtri,
pacpdate as od_dtco
from u8m_tr..pur10110
where status='OP' or status='IN' or status='NA')t;
truncate table INTERF_OFDE;
insert into INTERF_OFDE(od_serial,od_code,od_riga,od_item,od_qtor,od_qtev,od_umqt,od_dtor,od_dtri,od_dtco) 
select * from #t;
drop table #t;

----------INTERF_ODL(HEADER_PROD_ORD)----------
select IDENTITY(INT,1,1) as seq,
no+'-'+convert(varchar,seq) as ol_odlp,
nicim.dbo.RemoveCharCN(part) as ol_item,
nicim.dbo.RemoveCharCN(orderno+'-'+convert(varchar,orderseq)) as ol_cmve,
qty-recqty as ol_qtao,
reldate as ol_dtin,
orgactedate as ol_dtfi,
createdate as ol_dtri
into #t
from u8m_wx..mom10200
where left(no,3) in ('WMA','WMC','WMF','WMG','WMM','WMS','WSM') and status!='9';
truncate table INTERF_ODL;
insert into INTERF_ODL(ol_serial,ol_odlp,ol_item,ol_cmve,ol_qtao,ol_dtin,ol_dtfi,ol_dtri) 
select * from #t;
drop table #t;

----------INTERF_BOM(DETAIL_PROD_ORD)----------
select IDENTITY(INT,1,1) as seq,
mono+'-'+convert(varchar,moseq) as mo_odlp,
t1.part as mo_item,
t1.duedate as mo_dtmo,
t1.qty-t1.issqty as mo_qtaf
into #t
from u8m_wx..mom10320 t1,u8m_wx..mom10200 t2
where t1.mono=t2.no and t1.moseq=t2.seq and left(t2.no,3) in ('WMA','WMC','WMF','WMG','WMM','WMS','WSM') and t2.status!='9';
truncate table INTERF_BOM;
insert into INTERF_BOM(mo_serial,mo_odlp,mo_item,mo_dtmo,mo_qtaf) 
select * from #t;
drop table #t;

----------INTERF_GIAC(STOCK)----------
select IDENTITY(INT,1,1) as seq,
t.gi_item,
gi_lott,
gi_qtag
into #t from
(select t1.part as gi_item,
t2.orderno+'-'+convert(varchar,t2.orderseq) as gi_lott,
sum(onhand) as gi_qtag
from u8m_wx..inv10104 t1,u8m_wx..mom10200 t2
where t1.part*=t2.part and t1.lotno*=t2.userdef1 and t1.onhand!=0 
and t1.prloc in (select prloc from openrowset('SQLOLEDB','172.17.0.6\u8m';'sa';'',u8m_wx.dbo.sys10240) where mrpflag='Y')
group by t2.orderno+'-'+convert(varchar,t2.orderseq),t1.part
union all
select t1.part as gi_item,
t2.orderno+'-'+convert(varchar,t2.orderseq) as gi_lott,
sum(onhand) as gi_qtag
from u8m_tr..inv10104 t1,u8m_tr..mom10200 t2
where t1.part*=t2.part and t1.lotno*=t2.userdef1 and t1.onhand!=0 
and t1.prloc in (select prloc from openrowset('SQLOLEDB','172.17.0.5\u8m';'sa';'',u8m_tr.dbo.sys10240) where mrpflag='Y')
group by t2.orderno+'-'+convert(varchar,t2.orderseq),t1.part)t;
truncate table INTERF_GIAC;
insert into INTERF_GIAC(gi_serial,gi_item,gi_lott,gi_qtag) 
select * from #t;
drop table #t;
update INTERF_GIAC set GI_LOTT=null
where GI_ITEM collate chinese_prc_ci_as in(select part from openrowset('SQLOLEDB','172.17.0.5\u8m';'pcsamin';'Mappetsci0',nicim.dbo.masterdata) where GLP=3);
update INTERF_GIAC set GI_LOTT='TCKSB0398-1' where GI_ITEM='ES2730950C03933' and GI_QTAG='2.0000';
update INTERF_GIAC set GI_LOTT='TCHRB0063-10' where GI_ITEM='FNC4J44VA00257' and GI_QTAG='1.0000';
update INTERF_GIAC set GI_LOTT=null where GI_ITEM in ('XEA273P1AA0179','XEB273P1AA0197');

----------INTERF_FOR(SUPP_MASTER)----------
select IDENTITY(INT,1,1) as seq,
vendor as fo_cocl,
nicim.dbo.RemoveCharCN(userdef1) as fo_rags,
userdef2 as fo_cu01 
into #t
from u8m_wx..ven10100;
truncate table INTERF_FOR;
insert into INTERF_FOR(fo_serial,fo_cocl,fo_rags,fo_cu01) 
select * from #t;
drop table #t;

----------INTERF_CLI(CUST_MASTER)----------
select IDENTITY(INT,1,1) as seq,
t.cl_cocl,
t.cl_rags
into #t from 
(select 'T'+cust as cl_cocl,
nicim.dbo.RemoveCharCN(userdef1) as cl_rags
from u8m_tr..cst10100
union all
select 'W'+cust as cl_cocl,
nicim.dbo.RemoveCharCN(userdef1) as cl_rags
from u8m_wx..cst10100)t;
truncate table INTERF_CLI;
insert into INTERF_CLI(cl_serial,cl_cocl,cl_rags) 
select * from #t;
drop table #t;