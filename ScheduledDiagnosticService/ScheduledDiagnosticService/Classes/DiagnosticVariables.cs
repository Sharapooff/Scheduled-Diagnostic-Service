using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Classes
{
    struct Zapis_Ohlazhd_radiat_nasos       //алгоритм воздухоснабж.дизеля *1-7*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string soob1 { get; set; }
        public string soob2 { get; set; }
        public string soob3 { get; set; }
        public string soob4 { get; set; }
        public string soob5 { get; set; }
        public string soob6 { get; set; }
        public string soob7 { get; set; }
        public string soob8 { get; set; }
        public string soob9 { get; set; }
        public string soob10 { get; set; }
        public string k1 { get; set; }
        public string k2 { get; set; }
        public string S1 { get; set; }
        public string S2 { get; set; }
        public string ChVKV { get; set; }
    }

    struct Zapis_Vozduh         //алгоритм воздухоснабж.дизеля *1-6*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string Tokr { get; set; }
        public string Pnadd { get; set; }
        public string k_razn_nadd { get; set; }
        public string TLC { get; set; }
        public string TPC { get; set; }
        public string soob1 { get; set; }
        public string soob2 { get; set; }
        public string soob3 { get; set; }
        public string soob4 { get; set; }
        public string soob5 { get; set; }
    }

    struct Zapis_Akkum_batar          //алгоритм аккумуляторной батареи *6-1*
    {
        public string DAT { get; set; }
        public string I_SG_sum { get; set; }
        public string U_AB_sum { get; set; }
        public string I_kv_sum { get; set; }
        public string pr_IU { get; set; }
        public string kol_izm { get; set; }
        public string T_OkrSr { get; set; }
        public string T_HolSp { get; set; }
        public string T_vozd { get; set; }
        public string R { get; set; }
        public string E { get; set; }
        public string C_nom { get; set; }
        //public string C { get; set; }
        //public string E_procent { get; set; }
        public string soob1 { get; set; }
    }

    struct Zapis_Akkum_batar_BS          //алгоритм аккумуляторной батареи *6-1*
    {
        public string DAT { get; set; }
        public string I_SG_sum { get; set; }
        public string U_AB_sum { get; set; }
        public string I_kv_sum { get; set; }
        public string pr_IU { get; set; }
        public string kol_izm { get; set; }
        public string T_OkrSr { get; set; }
        public string T_vozd { get; set; }
        public string K { get; set; }
        public string C { get; set; }
        public string C_ab { get; set; }
        public string soob1 { get; set; }
    }

    struct Zapis_Raspred_I          //алгоритм увеличение скорости КП1-КП6 *5-1*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string razbrosI1_8 { get; set; }
        public string razbrosI2_8 { get; set; }
        public string razbrosI3_8 { get; set; }
        public string razbrosI4_8 { get; set; }
        public string razbrosI5_8 { get; set; }
        public string razbrosI6_8 { get; set; }
        public string razbrosI1_12 { get; set; }
        public string razbrosI2_12 { get; set; }
        public string razbrosI3_12 { get; set; }
        public string razbrosI4_12 { get; set; }
        public string razbrosI5_12 { get; set; }
        public string razbrosI6_12 { get; set; }
        public string soob1 { get; set; }
    }
    struct Zapis_Uscor_KP           //алгоритм увеличение скорости КП1-КП6 *5-1*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string uscorKP1 { get; set; }
        public string uscorKP2 { get; set; }
        public string uscorKP3 { get; set; }
        public string uscorKP4 { get; set; }
        public string uscorKP5 { get; set; }
        public string uscorKP6 { get; set; }
    }
    struct Zapis_Ited           //алгоритм превышение тока, напряжения *5-1*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string Ited1 { get; set; }
        public string Ited2 { get; set; }
        public string Ited3 { get; set; }
        public string Ited4 { get; set; }
        public string Ited5 { get; set; }
        public string Ited6 { get; set; }
        public string U { get; set; }
    }
    struct Zapis_Rashod_topl            //алгоритм давление дизельного топлива *4-2*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string delta_massa { get; set; }
        public string vremia { get; set; }
        public string rashod { get; set; }
        public string PTG { get; set; }
    }
    struct Zapis_FTOT_TNVD          //алгоритм давление дизельного топлива *4-1*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string FTOT { get; set; }
        public string TNVD { get; set; }
        public string ChVKV { get; set; }
        public string soob1 { get; set; }
    }
    struct Zapis_Holod_ustr         //алгоритм холодильная установка *3-1*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string delta_Tv_v_diz { get; set; }
        public string delta_Tv_hol_kont { get; set; }
        public string Tokr_sr { get; set; }
        public string delta_Time_str { get; set; }
        public string PTG { get; set; }
        public string soob1 { get; set; }
        public string soob2 { get; set; }
    }
    struct Zapis_TCil           //алгоритм температура цилиндров *2-3*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string ChvKV { get; set; }
        public string CilL { get; set; }
        public string CilL1 { get; set; }
        public string CilL2 { get; set; }
        public string CilL3 { get; set; }
        public string CilL4 { get; set; }
        public string CilL5 { get; set; }
        public string CilL6 { get; set; }
        public string CilL7 { get; set; }
        public string CilL8 { get; set; }
        public string CilP { get; set; }
        public string CilP1 { get; set; }
        public string CilP2 { get; set; }
        public string CilP3 { get; set; }
        public string CilP4 { get; set; }
        public string CilP5 { get; set; }
        public string CilP6 { get; set; }
        public string CilP7 { get; set; }
        public string CilP8 { get; set; }
        public string delta_cil { get; set; }
        public string delta_cil_L1 { get; set; }
        public string delta_cil_L2 { get; set; }
        public string delta_cil_L3 { get; set; }
        public string delta_cil_L4 { get; set; }
        public string delta_cil_L5 { get; set; }
        public string delta_cil_L6 { get; set; }
        public string delta_cil_L7 { get; set; }
        public string delta_cil_L8 { get; set; }
        public string delta_cil_P1 { get; set; }
        public string delta_cil_P2 { get; set; }
        public string delta_cil_P3 { get; set; }
        public string delta_cil_P4 { get; set; }
        public string delta_cil_P5 { get; set; }
        public string delta_cil_P6 { get; set; }
        public string delta_cil_P7 { get; set; }
        public string delta_cil_P8 { get; set; }
    }
    struct Zapis_Pmasla_MS          //алгоритм давление масла в мс *2-2*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string ChvKV { get; set; }
        public string Pm_vh_d { get; set; }
        public string Pm_vih_2nas { get; set; }
        public string deltaP { get; set; }
        public string Tm_vih_d { get; set; }
        public string soob1 { get; set; }
        public string soob2 { get; set; }
    }
    struct Zapis_Moschnost_diz          //алгоритм мощность дизеля *2-1*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string PKM_norm { get; set; }
        public string ChvKV { get; set; }
        public string PTG { get; set; }
        public string soob1 { get; set; }
        public string soob2 { get; set; }
    }
    struct Zapis_masl_sist          //алгоритм масляная система *1-5*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string soob1 { get; set; }
        public string soob2 { get; set; }
        public string iz { get; set; }
        public string kz { get; set; }
        public string Kol_izm { get; set; }
        public string sum_Pmvh { get; set; }
        public string sum_Pmvih2 { get; set; }
        public string sum_prPmvhnaPmvih2 { get; set; }
        public string sum_kvadsumPmvh { get; set; }
        public string sum_kvadsumPmvih2 { get; set; }
        public string Pvh { get; set; }
        public string Pvih2m { get; set; }
    }
    struct Zapis_topl_sist          //алгоритм топливная система *1-4*
    {
        public string DAT { get; set; }
        public string ChvKV { get; set; }
        public string TNVD { get; set; }
        public string FTOT { get; set; }
        public string deltaP { get; set; }
        public string PKM { get; set; }
        public string Regim { get; set; }
        public string soob { get; set; }
        public string znachenie { get; set; }
        public string Kol_izm { get; set; }
        public string pkm_zafixir { get; set; }
        public string sumFTOT { get; set; }
        public string sumTNVD { get; set; }
        public string prFTOTTNVD { get; set; }
        public string kvTNVD { get; set; }
        public string iz { get; set; }
    }
    struct Zapis_t_vod          //алгоритм температура воды *1-2*
    {
        public string DAT { get; set; }
        public string ChvKV { get; set; }
        public string T_vod { get; set; }
        public string PKM { get; set; }
        public string Regim { get; set; }
    }
    struct Zapis_t_mas          //алгоритм температура масла *1-3*
    {
        public string DAT { get; set; }
        public string ChvKV { get; set; }
        public string T_mas { get; set; }
        public string PKM { get; set; }
        public string Regim { get; set; }
    }
    struct Zapis_t_cil          //алгоритм температура цилиндров *1-1*
    {
        public string DAT { get; set; }
        public string PKM { get; set; }
        public string REJIM { get; set; }

        public string[] Cil;
    }
    struct KumSum       // *1-1*
    {
        public string dat { get; set; }
        public long[] Cil;
    }
    struct zap_1_2      // *1-1*
    {
        public string DAT { get; set; }
        public int PKM { get; set; }
        public int Tm { get; set; }
        public int Pin_d { get; set; }
        public int Pout_2n { get; set; }
    }
    struct Tabl         //*1-2*
    {
        public string datatime { get; set; }
        public string sms { get; set; }
        public string T_vod { get; set; }
    }
    struct Tabl_mas       //*1-3*
    {
        public string datatime { get; set; }
        public string sms { get; set; }
        public string T_mas { get; set; }
    }
    struct Tabl_ts      // *1-4*
    {
        public string datatime { get; set; }
        public string sms { get; set; }
        public string FTOT { get; set; }
        public string TNVD { get; set; }
        public string znach { get; set; }
        public string Izagr { get; set; }
    }
    struct Tabl_ms      // *1-5*
    {
        public string datatime { get; set; }
        public string sms { get; set; }
        public string pkm { get; set; }

        public string znach { get; set; }
        public string Izagr { get; set; }
        public string Kzagr { get; set; }
        public string Pvh { get; set; }
        public string Pvih2m { get; set; }
    }
    struct Tabl_md      // *2-1*
    {
        public string datatime { get; set; }
        public string sms { get; set; }
        public string PTG { get; set; }
        public string PTG_norm { get; set; }
        public string znach { get; set; }
        public string pkm { get; set; }
    }
    struct Tabl_Pm_ms      // *2-2*
    {
        public string datatime { get; set; }
        public string sms { get; set; }
        public string Pm_vh_d { get; set; }
        public string Pm_vih_2nas { get; set; }
        public string deltaP { get; set; }
        public string Tm_vih_d { get; set; }
        public string PKM { get; set; }

    }
    struct Tabl_TCil      // *2-3*
    {
        public string datatime { get; set; }
        public string sms { get; set; }
        public string PKM { get; set; }
        public string CilL { get; set; }
        public string CilL1 { get; set; }
        public string CilL2 { get; set; }
        public string CilL3 { get; set; }
        public string CilL4 { get; set; }
        public string CilL5 { get; set; }
        public string CilL6 { get; set; }
        public string CilL7 { get; set; }
        public string CilL8 { get; set; }
        public string CilP { get; set; }
        public string CilP1 { get; set; }
        public string CilP2 { get; set; }
        public string CilP3 { get; set; }
        public string CilP4 { get; set; }
        public string CilP5 { get; set; }
        public string CilP6 { get; set; }
        public string CilP7 { get; set; }
        public string CilP8 { get; set; }
        public string delta_cil { get; set; }
        public string delta_cil_L1 { get; set; }
        public string delta_cil_L2 { get; set; }
        public string delta_cil_L3 { get; set; }
        public string delta_cil_L4 { get; set; }
        public string delta_cil_L5 { get; set; }
        public string delta_cil_L6 { get; set; }
        public string delta_cil_L7 { get; set; }
        public string delta_cil_L8 { get; set; }
        public string delta_cil_P1 { get; set; }
        public string delta_cil_P2 { get; set; }
        public string delta_cil_P3 { get; set; }
        public string delta_cil_P4 { get; set; }
        public string delta_cil_P5 { get; set; }
        public string delta_cil_P6 { get; set; }
        public string delta_cil_P7 { get; set; }
        public string delta_cil_P8 { get; set; }
    }
    struct Tabl_HolodUstr      // *3-1*
    {
        public string datatime { get; set; }
        public string Tv_vih_diz { get; set; }
        public string Tv_hol_kont { get; set; }
        public string Tokr_sr { get; set; }
        public string vremia { get; set; }
        public string PKM { get; set; }
        public string PTG { get; set; }
    }
    struct Tabl_FtotTnvd      // *4-1*
    {
        public string datatime { get; set; }
        public string FTOT { get; set; }
        public string TNVD { get; set; }
        public string ChVKV { get; set; }
        public string PKM { get; set; }
    }
    struct Tabl_RashT      // *4-2*
    {
        public string datatime { get; set; }
        public string Massa { get; set; }
        public string Vremia { get; set; }
        public string PKM { get; set; }
        public string rashod { get; set; }
        public string PTG { get; set; }
    }
    struct Tabl_It      // *5-1*
    {
        public string datatime { get; set; }
        public string PKM { get; set; }
        public string I1 { get; set; }
        public string I2 { get; set; }
        public string I3 { get; set; }
        public string I4 { get; set; }
        public string I5 { get; set; }
        public string I6 { get; set; }
        public string U { get; set; }
    }
    struct Tabl_UscKP      // *5-2*
    {
        public string datatime { get; set; }
        public string PKM { get; set; }
        public string Usc1 { get; set; }
        public string Usc2 { get; set; }
        public string Usc3 { get; set; }
        public string Usc4 { get; set; }
        public string Usc5 { get; set; }
        public string Usc6 { get; set; }
    }
    struct Tabl_rasprI      // *5-3*
    {
        public string datatime { get; set; }
        public string PKM { get; set; }
        public string I1_8 { get; set; }
        public string I2_8 { get; set; }
        public string I3_8 { get; set; }
        public string I4_8 { get; set; }
        public string I5_8 { get; set; }
        public string I6_8 { get; set; }
        public string I1_12 { get; set; }
        public string I2_12 { get; set; }
        public string I3_12 { get; set; }
        public string I4_12 { get; set; }
        public string I5_12 { get; set; }
        public string I6_12 { get; set; }
        public string sms { get; set; }
    }

    struct Tabl_Akk_bat      // *6-1*
    {
        public string datatime { get; set; }
        public string I_TG { get; set; }
        public string U_AB { get; set; }
        public string Temp { get; set; }
        public string R { get; set; }
        public string E { get; set; }
        public string C { get; set; }
        public string E_proc { get; set; }
    }

    struct Tabl_Akk_bat_BS      // *1-8*
    {
        public string datatime { get; set; }
        public string I_TG { get; set; }
        public string U_AB { get; set; }
        public string Temp { get; set; }
        public string K { get; set; }
        public string C { get; set; }
        public string C_ab { get; set; }
    }

    struct Tabl_vozd      // *1-6*
    {
        public string datatime { get; set; }
        public string PKM { get; set; }
        public string Tokr_sr { get; set; }
        public string Pnadd_v { get; set; }
        public string k_razn { get; set; }
        public string T_lc { get; set; }
        public string T_pc { get; set; }
        public string sms { get; set; }
    }

    struct Tabl_ohlazhd      // *1-7*
    {
        public string datatime { get; set; }
        public string PKM { get; set; }
        public string sms { get; set; }
        public string k1 { get; set; }
        public string k2 { get; set; }
        public string S1 { get; set; }
        public string S2 { get; set; }
        public string ChVKV { get; set; }
    }

}