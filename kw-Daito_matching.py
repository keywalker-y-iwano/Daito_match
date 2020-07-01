# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import paramiko
import mojimoji as moji
import string
import re
import csv
import logging
from decimal import Decimal, ROUND_HALF_UP

logging.basicConfig(level=logging.INFO)
logging.info('=========================start========================')

def sprit_dai_p(s):
    str_s = str(s)
    if not str_s:
        return ''
    else:
        str_s_list = str_s.split(',')
        return str_s_list[0]

def sprit_dai_s(s):
    str_s = str(s)
    if not str_s:
        return ''
    else:
        str_s_list = str_s.split(',')
        return str_s_list[1]

def sprit_dai_sum(s):
    str_s = str(s)
    if not str_s:
        return ''
    else:
        str_s_list = str_s.split(',')
        return str_s_list[2]

def sprit_store_ps(s):
    str_s = str(s)
    pattern_p = r'ﾊﾟﾁﾝｺ(.+?)台'
    pattern_s = r'ｽﾛｯﾄ(.+?)台'
    pattern_ps = r'ﾊﾟﾁｽﾛ(.+?)台'
    if not str_s:
        return ''
    else:
        p = re.findall(pattern_p, str_s)
        s = re.findall(pattern_s, str_s)
        ps = re.findall(pattern_ps, str_s)
    try:
        if not p:
            output_p = 0
        else:
            output_p = int(p[0])
        if not s:
            output_s = 0
        else:
            output_s = int(s[0])
        if not ps:
            output_ps = 0
        else:
            output_ps = int(ps[0])
        output_sum = output_p + output_s + output_ps
        if output_sum == 0:
            return ',,'
        else:
            return str(output_p) + ',' + str(output_s + output_ps) + ',' + str(output_sum)
    except ValueError:
        return ''

def calc_rate(s):
    if(len(s) == 1):
        try:
            if str(s[0]) != '':
                s[0] = float(s[0])
                return Decimal(str(s[0])).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            else:
                return ''
        except ValueError:
            return 0
    else:
        try:
            output = float(s[0])/float(s[1])
            output =  Decimal(str(output)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            return output
        except ValueError:
            return 0

def calc_han(s):
    output = s[0:int(len(s)/2)]
    return output

def add_zero(s):
    if '/' in s:
        date_temp = s.split('/')
        if len(date_temp) != 3:
            return ''
        if len(date_temp[1]) == 1:
            month = '0' + date_temp[1]
        else:
            month = date_temp[1]

        if len(date_temp[2]) == 1:
            day = '0' + date_temp[2]
        else:
            day = date_temp[2]
        date = str(date_temp[0]) + '/' + month + '/' + day
        return date
    else:
        return ''

def check_date(s):
    if '/' in s:
        return s
    else:
        return ''

def delete_brackets(s):
    """
    括弧と括弧内文字列を削除
    """
    """ brackets to zenkaku """
    table = {
        "(": "（",
        ")": "）",
        "<": "＜",
        ">": "＞",
        "{": "｛",
        "}": "｝",
        "[": "［",
        "]": "］"
    }
    for key in table.keys():
        s = s.replace(key, table[key])
    """ delete zenkaku_brackets """
    l = ['（[^（|^）]*）', '【[^【|^】]*】', '＜[^＜|^＞]*＞', '［[^［|^］]*］',
         '「[^「|^」]*」', '｛[^｛|^｝]*｝', '〔[^〔|^〕]*〕', '〈[^〈|^〉]*〉']
    for l_ in l:
        s = re.sub(l_, "", s)
    """ recursive processing """
    return delete_brackets(s) if sum([1 if re.search(l_, s) else 0 for l_ in l]) > 0 else s

def read_csv(self, filepath, index_col=None):
    sftp = self.client.open_sftp()
    with sftp.open(filepath, "r") as f:
         df = pd.read_csv(f, encoding="utf-8", index_col=index_col)
    return df


dat = datetime.datetime.now()
date = dat.strftime('%Y/%m/%d')
dir_date = dat.strftime('%Y%m%d')
dir_date = dir_date[2:8]

logging.info('=====================Connect:GCP======================')

# SFTP接続先の設定
HOST = '104.198.89.41'
PORT = 'ssh'
USER = 'y_iwano'
KEY_FILE  = './id_rsa'
# 秘密鍵ファイルからキーを取得
rsa_key = paramiko.RSAKey.from_private_key_file(KEY_FILE)
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    #SFTPセッション開始
    client.connect(HOST, PORT, USER, pkey=rsa_key) # キーを指定することでパスワードは必要なし
    sftp_connection = client.open_sftp()
    with sftp_connection.open("/home/crawler/file/p-world_kisyu.csv", "r") as f:
        PW_model_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/p-world_dai.csv", "r") as f:
        PW_table_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/p-world_tenpo.csv", "r") as f:
        PW_store_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/dmm_kisyu.csv", "r") as f:
        DM_model_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/dmm_dai.csv", "r") as f:
        DM_table_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/dmm_tenpo.csv", "r") as f:
        DM_store_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/y_iwano/Daito/Pair_data/pair_tenpo.csv", "r") as f:
        store_pair_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/y_iwano/Daito/Pair_data/pair_kisyu.csv", "r") as f:
        model_pair_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
finally:
    client.close()

logging.info('=======================機種データ=======================')
logging.info('===============clensing : DM_model_array==============')

DM_model_array_clensing = DM_model_array.copy()
DM_model_array_clensing['clensing_title'] = ''
DM_model_array_clensing['maker'] = DM_model_array_clensing['maker'].str.replace('の掲載機種一覧','')
DM_model_array_clensing['maker'] = DM_model_array_clensing['maker'].str.replace(' ','')
DM_model_array_clensing['maker'] = DM_model_array_clensing['maker'].apply(calc_han)
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['title'].apply(moji.zen_to_han)
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.replace(' ','').str.replace('ⅰ','i').str.replace('ⅱ','ii').str.replace('ⅲ','iii').str.replace('ⅳ','iv').str.replace('ⅴ','v').str.replace('ⅵ','vi').str.replace('ⅶ','vii').str.replace('ⅷ','viii').str.replace('ⅸ','ix').str.replace('ⅹ','x')
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.lower()
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('･','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('‐','')
DM_model_array_clensing['d_date'] = DM_model_array_clensing['info5'].apply(delete_brackets)
DM_model_array_clensing['s_date'] = DM_model_array_clensing['d_date'].apply(moji.zen_to_han)
DM_model_array_clensing['s_date'] = DM_model_array_clensing['s_date'].apply(delete_brackets)
DM_model_array_clensing['s_date'] = ''
DM_model_array_clensing['s_date'] = DM_model_array_clensing['s_date'].str.replace('上旬予定','').str.replace('予定','')
DM_model_array_clensing['s_date'] = DM_model_array_clensing['s_date'].apply(add_zero)
DM_model_array_clensing['info1'] = DM_model_array_clensing['info1'].str.replace('件','').str.replace('(','').str.replace(')','')
DM_model_array_clensing['site'] = 1

logging.info('===============clensing : PW_model_array==============')

PW_model_array_clensing = PW_model_array.copy()
PW_model_array_clensing['clensing_title'] = ''
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['title'].apply(moji.zen_to_han)
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.replace(' ','').str.replace('ⅰ','i').str.replace('ⅱ','ii').str.replace('ⅲ','iii').str.replace('ⅳ','iv').str.replace('ⅴ','v').str.replace('ⅵ','vi').str.replace('ⅶ','vii').str.replace('ⅷ','viii').str.replace('ⅸ','ix').str.replace('ⅹ','x')
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.lower()
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('･','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('‐','')
PW_model_array_clensing['d_date'] = PW_model_array_clensing['d_date'].str.replace('導入開始：','')
PW_model_array_clensing['s_date'] = PW_model_array_clensing['s_date'].str.replace('調査日：','')
PW_model_array_clensing['s_date'] = PW_model_array_clensing['s_date'].str.replace('上旬予定','').str.replace('予定','')
PW_model_array_clensing['s_date'] = PW_model_array_clensing['s_date'].apply(add_zero)
PW_model_array_clensing['site'] = 0

logging.info('===================check: model_list==================')

DM_model_array_clensing = DM_model_array_clensing.drop_duplicates(subset=['clensing_title','P_S'])
DM_model_array_merge = pd.merge(DM_model_array_clensing, model_pair_array, on='dmm_pcode', how='left')
PW_model_array_merge = pd.merge(PW_model_array_clensing, model_pair_array, on='pw_pcode', how='left')
PW_model_array_merge_dropDM = PW_model_array_merge.dropna(subset=['dmm_pcode_y'])

output_kisyu_p = pd.DataFrame(columns=['clensing_title','pcode', '機種名', 'Ｐ／Ｓ区分', '機種種別', 'タイプ', 'メーカー名', '機種コメント', '情報0', '内容0', '情報1', '内容1', '情報2', '内容2', '情報3', '内容3', '情報4', '内容4', '情報5', '内容5', '情報6', '内容6', '情報7', '内容7', '情報8', '内容8', '情報9', '内容9', '情報10', '内容10', '情報11', '内容11', '導入年月', '更新日付', 'dmm_pcode'])
output_kisyu_p['clensing_title'] = PW_model_array_merge_dropDM['clensing_title']
output_kisyu_p['pcode'] = 'p' + PW_model_array_merge_dropDM['pw_pcode']
output_kisyu_p['機種名'] = PW_model_array_merge_dropDM['title']
output_kisyu_p['Ｐ／Ｓ区分'] = PW_model_array_merge_dropDM['P_S']
output_kisyu_p['機種種別'] = PW_model_array_merge_dropDM['k_kind']
output_kisyu_p['メーカー名'] = PW_model_array_merge_dropDM['maker']
output_kisyu_p['情報0'] = '型式名'
output_kisyu_p['内容0'] = PW_model_array_merge_dropDM['info0']
output_kisyu_p['情報1'] = 'レビュー件数'
output_kisyu_p['内容1'] = PW_model_array_merge_dropDM['info1']
output_kisyu_p['情報2'] = 'レビュースコア'
output_kisyu_p['内容2'] = PW_model_array_merge_dropDM['info2']
output_kisyu_p['導入年月'] = PW_model_array_merge_dropDM['d_date']
output_kisyu_p['更新日付'] = PW_model_array_merge_dropDM['s_date']
output_kisyu_p['dmm_pcode'] = 'dmm_' + PW_model_array_merge_dropDM['dmm_pcode_y']


logging.info('===============duplicate : output_kisyu===============')

DM_model_duplicate = DM_model_array_merge[DM_model_array_merge['pw_pcode_y'].isnull()]
PW_model_duplicate = PW_model_array_merge[PW_model_array_merge['dmm_pcode_y'].isnull()]

model_array_output = pd.merge(PW_model_duplicate, DM_model_duplicate, on=['clensing_title','P_S'], how='left')
model_array_output = model_array_output.drop_duplicates(subset=['clensing_title','P_S'])
output_kisyu = pd.DataFrame(columns=['clensing_title','pcode', '機種名', 'Ｐ／Ｓ区分', '機種種別', 'タイプ', 'メーカー名', '機種コメント', '情報0', '内容0', '情報1', '内容1', '情報2', '内容2', '情報3', '内容3', '情報4', '内容4', '情報5', '内容5', '情報6', '内容6', '情報7', '内容7', '情報8', '内容8', '情報9', '内容9', '情報10', '内容10', '情報11', '内容11', '導入年月', '更新日付', 'dmm_pcode'])
output_kisyu['clensing_title'] = model_array_output['clensing_title']
output_kisyu['pcode'] = 'p' + model_array_output['pw_pcode']
output_kisyu['機種名'] = model_array_output['title_x']
output_kisyu['Ｐ／Ｓ区分'] = model_array_output['P_S']
output_kisyu['機種種別'] = model_array_output['k_kind_x']
output_kisyu['メーカー名'] = model_array_output['maker_x']
output_kisyu['情報0'] = '型式名'
output_kisyu['内容0'] = model_array_output['info0_x']
output_kisyu['情報1'] = 'レビュー件数'
output_kisyu['内容1'] = model_array_output['info1_y']
output_kisyu['情報2'] = 'レビュースコア'
output_kisyu['内容2'] = model_array_output['info2_y']
output_kisyu['導入年月'] = model_array_output['d_date_y']
output_kisyu['更新日付'] = model_array_output['s_date_x']
output_kisyu['dmm_pcode'] = 'dmm_' + model_array_output['dmm_pcode_y']

output_kisyu_to_csv = pd.concat([output_kisyu_p, output_kisyu])
output_kisyu_to_csv_temp = output_kisyu_to_csv
output_kisyu_to_csv = output_kisyu_to_csv.drop('clensing_title', axis=1)
output_kisyu_to_csv = output_kisyu_to_csv.drop_duplicates(subset=['機種名','pcode'])

logging.info('================output : output_kisyu_pair============')

model_output_array_inner = pd.merge(PW_model_duplicate, DM_model_duplicate, on='clensing_title', how='inner')
model_output_pair_array = pd.DataFrame(columns=['dmm_pcode','pw_pcode'])
model_output_pair_array['pw_pcode'] = model_output_array_inner['pw_pcode']
model_output_pair_array['dmm_pcode'] = model_output_array_inner['dmm_pcode']
output_pair_to_csv = pd.concat([model_pair_array, model_output_pair_array])

logging.info('==========duplicate : output_kisyu_not_match==========')

output_kisyu_to_csv_temp['pcode'] = model_array_output['pw_pcode'].str.replace('p','')
PW_model_not_match = output_kisyu_to_csv_temp[output_kisyu_to_csv_temp['dmm_pcode'].isnull()]
model_output_array_inner2 = pd.merge(PW_model_array_merge, DM_model_array_merge, on='clensing_title', how='inner')
model_output_array_inner2 = model_output_array_inner2.drop_duplicates(subset=['clensing_title','P_S_x'])
model_output_array_outer = pd.merge(PW_model_duplicate, DM_model_duplicate, on='clensing_title', how='outer')
model_output_array_outer['比較用の列'] = model_output_array_outer.apply(lambda x: '{}_{}'.format(x[0], x[35]), axis=1)
output_kisyu_to_csv_temp['比較用の列'] = output_kisyu_to_csv_temp.apply(lambda x: '{}_{}'.format(x[0], x[34]), axis=1)

PW_model_not_match = model_output_array_outer[~model_output_array_outer['比較用の列'].isin(output_kisyu_to_csv_temp['比較用の列'])]
PW_model_not_match = PW_model_not_match.drop_duplicates(subset=['clensing_title','P_S_x'])
PW_model_not_match['info1_y'] = PW_model_not_match['info1_x'].str.replace('件','').str.replace('（','').str.replace('）','')
PW_model_not_match['d_date_x'] = PW_model_not_match['d_date_x'].fillna('')
PW_model_not_match['d_date_x'] = PW_model_not_match['d_date_x'].apply(check_date)
PW_model_not_match = PW_model_not_match.dropna(subset=['site_x'])

DM_model_not_match = model_output_array_outer[~model_output_array_outer['比較用の列'].isin(output_kisyu_to_csv_temp['比較用の列'])]
DM_model_not_match = DM_model_not_match.drop_duplicates(subset=['clensing_title','P_S_y'])
DM_model_not_match['info1_y'] = DM_model_not_match['info1_y'].str.replace('件','').str.replace('（','').str.replace('）','')
DM_model_not_match['d_date_y'] = DM_model_not_match['d_date_y'].fillna('')
DM_model_not_match['d_date_y'] = DM_model_not_match['d_date_y'].apply(check_date)
DM_model_not_match = DM_model_not_match.dropna(subset=['site_y'])

output_kisyu_not_match_PW = pd.DataFrame(columns=['pcode', '機種名', 'Ｐ／Ｓ区分', '機種種別', 'タイプ', 'メーカー名', '機種コメント', '情報0', '内容0', '情報1', '内容1', '情報2', '内容2', '情報3', '内容3', '情報4', '内容4', '情報5', '内容5', '情報6', '内容6', '情報7', '内容7', '情報8', '内容8', '情報9', '内容9', '情報10', '内容10', '情報11', '内容11', '導入年月', '更新日付', 'dmm_pcode'])
output_kisyu_not_match_PW['pcode'] = 'p' + PW_model_not_match['pw_pcode']
output_kisyu_not_match_PW['機種名'] = PW_model_not_match['title_x']
output_kisyu_not_match_PW['Ｐ／Ｓ区分'] = PW_model_not_match['P_S_x']
output_kisyu_not_match_PW['機種種別'] = PW_model_not_match['k_kind_x']
output_kisyu_not_match_PW['メーカー名'] = PW_model_not_match['maker_x']
output_kisyu_not_match_PW['情報0'] = '型式名'
output_kisyu_not_match_PW['内容0'] = PW_model_not_match['info0_x']
output_kisyu_not_match_PW['情報1'] = 'レビュー件数'
output_kisyu_not_match_PW['情報1'] = PW_model_not_match['info1_x']
output_kisyu_not_match_PW['情報2'] = 'レビュースコア'
output_kisyu_not_match_PW['内容2'] = PW_model_not_match['info2_x']
output_kisyu_not_match_PW['導入年月'] = PW_model_not_match['d_date_x']
output_kisyu_not_match_PW['更新日付'] = PW_model_not_match['s_date_x']

output_kisyu_not_match_DM = pd.DataFrame(columns=['pcode', '機種名', 'Ｐ／Ｓ区分', '機種種別', 'タイプ', 'メーカー名', '機種コメント', '情報0', '内容0', '情報1', '内容1', '情報2', '内容2', '情報3', '内容3', '情報4', '内容4', '情報5', '内容5', '情報6', '内容6', '情報7', '内容7', '情報8', '内容8', '情報9', '内容9', '情報10', '内容10', '情報11', '内容11', '導入年月', '更新日付', 'dmm_pcode'])
output_kisyu_not_match_DM['機種名'] = DM_model_not_match['title_y']
output_kisyu_not_match_DM['Ｐ／Ｓ区分'] = DM_model_not_match['P_S_y']
output_kisyu_not_match_DM['機種種別'] = DM_model_not_match['k_kind_y']
output_kisyu_not_match_DM['メーカー名'] = DM_model_not_match['maker_y']
output_kisyu_not_match_DM['情報0'] =  '型式名'
output_kisyu_not_match_DM['内容0'] = DM_model_not_match['info0_y']
output_kisyu_not_match_DM['情報1'] = 'レビュー件数'
output_kisyu_not_match_DM['内容1'] = DM_model_not_match['info1_y']
output_kisyu_not_match_DM['情報2'] = 'レビュースコア'
output_kisyu_not_match_DM['内容2'] = DM_model_not_match['info2_y']
output_kisyu_not_match_DM['導入年月'] = DM_model_not_match['d_date_y']
output_kisyu_not_match_DM['更新日付'] = DM_model_not_match['s_date_y']
output_kisyu_not_match_DM['dmm_pcode'] = 'dmm_' + DM_model_not_match['dmm_pcode']


logging.info('=========================台データ=======================')
logging.info('================clensing : DM_table_array=============')

DM_table_array_clensing = DM_table_array.copy()
DM_table_array_clensing['clensing_title'] = ''
DM_table_array_clensing['co_date'] = ''

DM_table_array_clensing['p_code'] = 'p' + DM_table_array_clensing['p_code']
DM_table_array_clensing['num'] = DM_table_array_clensing['num'].str.replace('台','')
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate'].apply(moji.zen_to_han)
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate'].str.replace(' ','').str.replace('台','').str.replace('S','').str.replace('[','').str.replace('金額','').str.replace(']','').str.replace('【','').str.replace('】','').str.replace('円','').str.replace('し','').str.replace('貸','').str.replace('玉','').str.replace('枚','').str.replace('ﾊﾟﾁ','').str.replace('ﾝｺ','').str.replace('ｽﾛ','').str.replace('ｯﾄ','').str.replace('貸しS','').str.replace('しS','').str.replace('¥','').str.replace('ﾗﾌｪ⑤','').str.replace('ﾗﾌｪ','').str.replace('②','').str.replace('⓻','').str.replace(',','').str.replace('ｲﾁ','').str.replace('甘ﾃﾞｼﾞ･羽根物','').str.replace('ぱち','').str.replace('ﾐﾄﾞﾙ･ﾗｲﾄ','')
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate'].str.replace('=','/').str.replace('‐','/').str.replace(':','/').str.replace('階','/')
DM_table_array_clensing['rate_list'] = DM_table_array_clensing['rate'].str.split('/')
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate_list'].apply(calc_rate)
DM_table_array_clensing['co_date'] = date
DM_table_array_clensing = DM_table_array_clensing.drop('rate_list', axis=1)

DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['p_title'].fillna('')
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].apply(moji.zen_to_han)
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.replace(' ','').str.replace('Ⅰ','I').str.replace('Ⅱ','II').str.replace('Ⅲ','III').str.replace('Ⅳ','IV').str.replace('Ⅴ','V').str.replace('Ⅵ','VI').str.replace('Ⅶ','VII').str.replace('Ⅷ','VIII').str.replace('Ⅸ','IX').str.replace('Ⅹ','X')
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.lower()
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('☆','').str.replace('†','').str.replace('…','')
DM_table_array_clensing['site'] = 1

logging.info('===============clensing : PW_table_array==============')

PW_table_array_clensing = PW_table_array.copy()
PW_table_array_clensing['clensing_title'] = ''
PW_table_array_clensing['co_date'] = ''

PW_table_array_clensing['p_code'] = 'p' + PW_table_array_clensing['p_code']
PW_table_array_clensing['num'] = PW_table_array_clensing['num'].str.replace('台','')
PW_table_array_clensing['pw_t_code'] = PW_table_array_clensing['pw_t_code'].str.replace('.htm','')
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate'].fillna('')
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate'].apply(moji.zen_to_han)
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate'].str.replace(' ','').str.replace('[','').str.replace(']','').str.replace('【','').str.replace('】','').str.replace('円','').str.replace('貸','').str.replace('玉','').str.replace('枚','').str.replace('ﾊﾟﾁ','').str.replace('ﾝｺ','').str.replace('ｽﾛ','').str.replace('ｯﾄ','').str.replace('貸しS','').str.replace('しS','').str.replace('¥','').str.replace('ﾗﾌｪ⑤','').str.replace('ﾗﾌｪ','').str.replace('②','').str.replace('⓻','').str.replace(',','')
PW_table_array_clensing['rate_list'] = PW_table_array_clensing['rate'].str.split('/')
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate_list'].apply(calc_rate)
PW_table_array_clensing['co_date'] = date
PW_table_array_clensing = PW_table_array_clensing.drop('rate_list', axis=1)

PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['p_title'].fillna('')
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].apply(moji.zen_to_han)
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.replace(' ','').str.replace('Ⅰ','I').str.replace('Ⅱ','II').str.replace('Ⅲ','III').str.replace('Ⅳ','IV').str.replace('Ⅴ','V').str.replace('Ⅵ','VI').str.replace('Ⅶ','VII').str.replace('Ⅷ','VIII').str.replace('Ⅸ','IX').str.replace('Ⅹ','X')
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.lower()
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('☆','').str.replace('†','').str.replace('…','')
PW_table_array_clensing['site'] = 0

logging.info('==================marge : table_array=================')

table_array_innner = pd.merge(PW_table_array_clensing, DM_table_array_clensing, on=['clensing_title', 'rate', 'state_cd', 'p_code'], how='left')

PW_table_array_output = table_array_innner.dropna(subset=['site_x'])
DM_table_array_output = table_array_innner.dropna(subset=['site_y'])

output_dai_p = pd.DataFrame(columns=['state_cd', 't_code', 'pcode', '正式機種名', '機種名(店舗入力名)', '設置台数', '貸玉量', '更新日付'])
output_dai_d = pd.DataFrame(columns=['state_cd', 't_code', 'pcode', '正式機種名', '機種名(店舗入力名)', '設置台数', '貸玉量', '更新日付'])

output_dai_p['state_cd'] = PW_table_array_output['state_cd']
output_dai_p['t_code'] = PW_table_array_output['pw_t_code']
output_dai_p['pcode'] = PW_table_array_output['p_code']
output_dai_p['正式機種名'] = PW_table_array_output['p_title_x']
output_dai_p['機種名(店舗入力名)'] = PW_table_array_output['p_title_x']
output_dai_p['設置台数'] = PW_table_array_output['num_x']
output_dai_p['貸玉量'] = PW_table_array_output['rate']
output_dai_p['更新日付'] = PW_table_array_output['co_date_x']

output_dai_d['state_cd'] = DM_table_array_output['state_cd']
output_dai_d['t_code'] = DM_table_array_output['pw_t_code']
output_dai_d['pcode'] = DM_table_array_output['p_code']
output_dai_d['機種名(店舗入力名)'] = DM_table_array_output['p_title_y']
output_dai_d['設置台数'] = DM_table_array_output['num_x']
output_dai_d['貸玉量'] = DM_table_array_output['rate']
output_dai_d['更新日付'] = DM_table_array_output['co_date_y']

output_dai_to_csv = pd.concat([output_dai_p, output_dai_d],sort=True)
output_dai_to_csv = output_dai_to_csv.loc[:, ['state_cd', 't_code', 'pcode', '正式機種名', '機種名(店舗入力名)', '設置台数', '貸玉量', '更新日付']]



logging.info('=======================店舗データ=======================')
logging.info('================clensing : DM_store_array=============')

DM_store_array_clensing = DM_store_array.copy()
DM_store_array_clensing['clensing_title'] = ''
DM_store_array_clensing['address'] = ''
DM_store_array_clensing['address'] = DM_store_array_clensing['adress']
DM_store_array_clensing['address'] = DM_store_array_clensing['address'].str.replace(' ','').str.replace('\n','').str.replace('大きな地図で見る','')
DM_store_array_clensing['clensing_title'] = DM_store_array_clensing['name'].apply(delete_brackets)
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].fillna('')
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].apply(delete_brackets)
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].str.replace('更新日:','')
DM_store_array_clensing['clensing_title'] = DM_store_array_clensing['name'].apply(delete_brackets)
DM_store_array_clensing['parking'] = DM_store_array_clensing['parking'].apply(moji.zen_to_han)
DM_store_array_clensing['parking'] = DM_store_array_clensing['parking'].str.replace('　','').str.replace('\\','').str.replace('n','').str.replace('台','')
DM_store_array_clensing['t_code'] = DM_store_array_clensing['dmm_t_code']
DM_store_array_clensing['co_date'] = date
DM_store_array_clensing = DM_store_array_clensing.drop_duplicates(subset='t_code')
DM_store_array_clensing = DM_store_array_clensing.drop_duplicates(subset=['clensing_title','state_cd'])
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai'].fillna('')
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].apply(moji.zen_to_han)
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].str.replace(' ','').str.replace(':','')
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].str.replace('/','').str.replace('-','0台')
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].apply(delete_brackets)
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].apply(sprit_store_ps)
DM_store_array_clensing['dai_p'] = DM_store_array_clensing['dai_sprit'].apply(sprit_dai_p)
DM_store_array_clensing['dai_s'] = DM_store_array_clensing['dai_sprit'].apply(sprit_dai_s)
DM_store_array_clensing['dai_sum'] = DM_store_array_clensing['dai_sprit'].apply(sprit_dai_sum)
DM_store_array_clensing['site'] = 1

logging.info('================clensing : PW_store_array=============')

PW_store_array_clensing = PW_store_array.copy()
PW_store_array_clensing['clensing_title'] = ''
PW_store_array_clensing['address'] = ''
PW_store_array_clensing['address'] = PW_store_array_clensing['adress']
PW_store_array_clensing['address'] = PW_store_array_clensing['address'].str.replace(' ','').str.replace('\\n','').str.replace('大きな地図で見る','')
PW_store_array_clensing['clensing_title'] = PW_store_array_clensing['name'].apply(delete_brackets)
PW_store_array_clensing['parking'] = PW_store_array_clensing['parking'].str.replace('　','').str.replace('\\','').str.replace('n','').str.replace('台','')
PW_store_array_clensing['tenpo_update'] = PW_store_array_clensing['tenpo_update'].fillna('')
PW_store_array_clensing['tenpo_update'] = PW_store_array_clensing['tenpo_update'].apply(moji.zen_to_han)
PW_store_array_clensing['tenpo_update'] = PW_store_array_clensing['tenpo_update'].str.replace('皆様のご来店をお待ちしております｡','').str.replace('最終更新日','').str.replace(' ','').str.replace('<','').str.replace('>','')
PW_store_array_clensing['pw_t_code'] = PW_store_array_clensing['pw_t_code'].str.replace('.htm','')
PW_store_array_clensing['t_code'] = PW_store_array_clensing['pw_t_code'].str.replace('.htm','')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai'].fillna('')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].apply(moji.zen_to_han)
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].str.replace(' ','').str.replace(':','')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].str.replace('/','').str.replace('-','0台')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].apply(delete_brackets)
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].apply(sprit_store_ps)
PW_store_array_clensing['dai_p'] = PW_store_array_clensing['dai_sprit'].apply(sprit_dai_p)
PW_store_array_clensing['dai_s'] = PW_store_array_clensing['dai_sprit'].apply(sprit_dai_s)
PW_store_array_clensing['dai_sum'] = PW_store_array_clensing['dai_sprit'].apply(sprit_dai_sum)
PW_store_array_clensing['co_date'] = date


PW_store_array_clensing = PW_store_array_clensing.drop_duplicates(subset='pw_t_code')
PW_store_array_clensing = PW_store_array_clensing.drop_duplicates(subset=['clensing_title','state_cd'])
PW_store_array_clensing['site'] = 0


logging.info('===================check: Store_list==================')

store_pair_array = store_pair_array.rename(columns={'t_code':'pw_t_code'})
store_pair_array = store_pair_array.drop_duplicates(subset='pw_t_code')
store_pair_array = store_pair_array.drop_duplicates(subset='dmm_t_code')

PW_store_array_merge = pd.merge(PW_store_array_clensing, store_pair_array, on='pw_t_code', how='inner')
PW_store_array_merged = PW_store_array_merge.dropna(subset=['dmm_t_code_y'])

logging.info('===============duplicate : output_kisyu===============')

output_tenpo_pp = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_pp['state_cd'] = PW_store_array_merged['state_cd'] 
output_tenpo_pp['t_code'] =  PW_store_array_merged['pw_t_code'] 
output_tenpo_pp['ホール名'] =   PW_store_array_merged['name'] 
output_tenpo_pp['address'] =PW_store_array_merged['address'] 
output_tenpo_pp['access'] = PW_store_array_merged['access'] 
output_tenpo_pp['closeday'] =PW_store_array_merged['closeday'] 
output_tenpo_pp['opentime'] = PW_store_array_merged['opentime'] 
output_tenpo_pp['service'] = PW_store_array_merged['service'] 
output_tenpo_pp['rate'] = PW_store_array_merged['rate']
output_tenpo_pp['dai'] = PW_store_array_merged['dai'] 
output_tenpo_pp['dai_p'] = PW_store_array_merged['dai_p'] 
output_tenpo_pp['dai_s'] = PW_store_array_merged['dai_s'] 
output_tenpo_pp['dai_sum'] = PW_store_array_merged['dai_sum'] 
output_tenpo_pp['parking'] = PW_store_array_merged['parking'] 
output_tenpo_pp['tel'] = PW_store_array_merged['tel']
output_tenpo_pp['tenpo_update'] = PW_store_array_merged['tenpo_update'] 
output_tenpo_pp['co_date'] = PW_store_array_merged['co_date'] 
output_tenpo_pp['pw_t_code'] = PW_store_array_merged['pw_t_code'] 
output_tenpo_pp['dmm_t_code'] = 'dmm_' + PW_store_array_merged['dmm_t_code_y'] 
output_tenpo_pp['merge_url'] = PW_store_array_merged['url']
output_tenpo_pp['clensing_title'] = PW_store_array_merged['clensing_title'] 
output_tenpo_pp = output_tenpo_pp.drop_duplicates(subset=['clensing_title','state_cd'])

DM_store_array_merge = pd.merge(DM_store_array_clensing, store_pair_array, on='dmm_t_code', how='inner')
DM_store_array_merged = DM_store_array_merge.dropna(subset=['pw_t_code_y'])

output_tenpo_pd = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_pd['state_cd'] = DM_store_array_merged['state_cd']
output_tenpo_pd['t_code'] = 'dmm_' + DM_store_array_merged['dmm_t_code']
output_tenpo_pd['ホール名'] = DM_store_array_merged['name']
output_tenpo_pd['address'] = DM_store_array_merged['address']
output_tenpo_pd['access'] = DM_store_array_merged['access']
output_tenpo_pd['closeday'] = DM_store_array_merged['closeday']
output_tenpo_pd['opentime'] = DM_store_array_merged['opentime']
output_tenpo_pd['service'] = DM_store_array_merged['service']
output_tenpo_pd['rate'] = DM_store_array_merged['rate']
output_tenpo_pd['dai'] = DM_store_array_merged['dai']
output_tenpo_pd['dai_p'] = DM_store_array_merged['dai_p']
output_tenpo_pd['dai_s'] = DM_store_array_merged['dai_s']
output_tenpo_pd['dai_sum'] = DM_store_array_merged['dai_sum']
output_tenpo_pd['parking'] = DM_store_array_merged['parking']
output_tenpo_pd['tel'] = DM_store_array_merged['tel']
output_tenpo_pd['tenpo_update'] = DM_store_array_merged['tenpo_update']
output_tenpo_pd['co_date'] = DM_store_array_merged['co_date']
output_tenpo_pd['merge_url'] = DM_store_array_merged['url']
output_tenpo_pd['dmm_t_code'] = 'dmm_' + DM_store_array_merged['dmm_t_code']
output_tenpo_pd['clensing_title'] = DM_store_array_merged['clensing_title']

output_tenpo_pc = pd.merge(output_tenpo_pp, output_tenpo_pd, on=['clensing_title','state_cd'], how='inner')

output_tenpo_pc = output_tenpo_pc.rename(columns={'pw_t_code_x': 'pw_t_code'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'t_code_x': 't_code'})

store_list_inner = pd.merge(PW_store_array_clensing, output_tenpo_pc, on='pw_t_code', how='inner')
store_list_inner = store_list_inner.drop_duplicates(subset=['clensing_title_x','state_cd_x'])
store_list_inner['比較用の列'] = store_list_inner.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)

PW_store_array_clensing['比較用の列'] = PW_store_array_clensing.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)
PW_store_list_match = PW_store_array_clensing[~PW_store_array_clensing['比較用の列'].isin(store_list_inner['比較用の列'])]

output_tenpo_pc = output_tenpo_pc.rename(columns={'dmm_t_code_y': 'dmm_t_code'})
output_tenpo_pc['dmm_t_code'] = output_tenpo_pc['dmm_t_code'].str.replace('dmm_','')
store_list_inner = pd.merge(DM_store_array_clensing, output_tenpo_pc, on='dmm_t_code', how='inner')
store_list_inner = store_list_inner.drop_duplicates(subset='dmm_t_code')
store_list_inner['比較用の列'] = store_list_inner.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)
DM_store_array_clensing['比較用の列'] = DM_store_array_clensing.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)
DM_store_list_match = DM_store_array_clensing[~DM_store_array_clensing['比較用の列'].isin(store_list_inner['比較用の列'])]

DM_store_duplicate = DM_store_list_match
PW_store_duplicate = PW_store_list_match

store_array_output = pd.merge(PW_store_duplicate, DM_store_duplicate, on=['clensing_title','state_cd'], how='inner')
store_array_output = store_array_output.drop_duplicates(subset=['clensing_title','state_cd'])

pair_tenpo = pd.DataFrame(columns=['t_code', 'dmm_t_code'])
pair_tenpo['t_code'] = store_array_output['pw_t_code_x']
pair_tenpo['dmm_t_code'] = store_array_output['dmm_t_code_y']
store_pair_array = store_pair_array.rename(columns={'pw_t_code':'t_code'})
output_pair = pd.concat([store_pair_array,pair_tenpo])

store_array_output = pd.merge(PW_store_duplicate, DM_store_duplicate, on=['clensing_title','state_cd'], how='outer')

PW_store_array_output = store_array_output.dropna(subset=['site_x'])
DM_store_array_output = store_array_output.dropna(subset=['site_y'])

output_tenpo_p = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_d = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])

output_tenpo_p['state_cd'] = PW_store_array_output['state_cd']
output_tenpo_p['t_code'] = PW_store_array_output['pw_t_code_x']
output_tenpo_p['ホール名'] = PW_store_array_output['name_x']
output_tenpo_p['address'] = PW_store_array_output['address_x']
output_tenpo_p['access'] = PW_store_array_output['access_x']
output_tenpo_p['closeday'] = PW_store_array_output['closeday_x']
output_tenpo_p['opentime'] = PW_store_array_output['opentime_x']
output_tenpo_p['service'] = PW_store_array_output['service_x']
output_tenpo_p['rate'] = PW_store_array_output['rate_x']
output_tenpo_p['dai'] = PW_store_array_output['dai_x']
output_tenpo_p['dai_p'] = PW_store_array_output['dai_p_x']
output_tenpo_p['dai_s'] = PW_store_array_output['dai_s_x']
output_tenpo_p['dai_sum'] = PW_store_array_output['dai_sum_x']
output_tenpo_p['parking'] = PW_store_array_output['parking_x']
output_tenpo_p['tel'] = PW_store_array_output['tel_x']
output_tenpo_p['tenpo_update'] = PW_store_array_output['tenpo_update_x']
output_tenpo_p['co_date'] = PW_store_array_output['co_date_x']
output_tenpo_p['merge_url'] = PW_store_array_output['url_x']
output_tenpo_p['pw_t_code'] = PW_store_array_output['pw_t_code_x']

output_tenpo_d['state_cd'] = DM_store_array_output['state_cd']
output_tenpo_d['t_code'] = 'dmm_' + DM_store_array_output['dmm_t_code_y']
output_tenpo_d['ホール名'] = DM_store_array_output['name_y']
output_tenpo_d['address'] = DM_store_array_output['address_y']
output_tenpo_d['access'] = DM_store_array_output['access_y']
output_tenpo_d['closeday'] = DM_store_array_output['closeday_y']
output_tenpo_d['opentime'] = DM_store_array_output['opentime_y']
output_tenpo_d['service'] = DM_store_array_output['service_y']
output_tenpo_d['rate'] = DM_store_array_output['rate_y']
output_tenpo_d['dai'] = DM_store_array_output['dai_y']
output_tenpo_d['dai_p'] = DM_store_array_output['dai_p_y']
output_tenpo_d['dai_s'] = DM_store_array_output['dai_s_y']
output_tenpo_d['dai_sum'] = DM_store_array_output['dai_sum_y']
output_tenpo_d['parking'] = DM_store_array_output['parking_y']
output_tenpo_d['tel'] = DM_store_array_output['tel_y']
output_tenpo_d['merge_url'] = DM_store_array_output['url_y']
output_tenpo_d['tenpo_update'] = DM_store_array_output['tenpo_update_x']
output_tenpo_d['co_date'] = DM_store_array_output['co_date_y']
output_tenpo_d['dmm_t_code'] = 'dmm_' + DM_store_array_output['dmm_t_code_y']

output_tenpo_pc = output_tenpo_pc.rename(columns={'総務省コード_x' : '総務省コード'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'ホール名_x' : 'ホール名'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'address_x' : 'address'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'access_x' : 'access'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'closeday_x' : 'closeday'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'opentime_x' : 'opentime'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'service_x' : 'service'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'rate_x' : 'rate'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'dai_x' : 'dai'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'dai_p_x' : 'dai_p'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'dai_s_x' : 'dai_s'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'dai_sum_x' : 'dai_sum'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'parking_x' : 'parking'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'tel_x' : 'tel'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'url_y' : 'url'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'tenpo_update_x' : 'tenpo_update'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'merge_url_x' : 'merge_url'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'tel_x' : 'tel'})
output_tenpo_pc = output_tenpo_pc.rename(columns={'co_date_x' : 'co_date'})
output_tenpo_pc['dmm_t_code'] = 'dmm_' + output_tenpo_pc['dmm_t_code']

output_tenpo_to_csv = pd.concat([output_tenpo_pc, output_tenpo_p,output_tenpo_d],sort=True)
output_tenpo_to_csv = output_tenpo_to_csv.loc[:, ['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code']]

output_tenpo_not_match = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_not_match_dm = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])

pair_tenpo = pd.DataFrame(columns=['t_code', 'dmm_t_code'])

logging.info('==========duplicate : Output_Kisyu_Not_Match==========')


output_tenpo_not_match_pw = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_not_match_dm = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])

output_tenpo_not_match_pw['state_cd'] = PW_store_list_match['state_cd']
output_tenpo_not_match_pw['t_code'] = PW_store_list_match['pw_t_code']
output_tenpo_not_match_pw['ホール名'] = PW_store_list_match['name']
output_tenpo_not_match_pw['address'] = PW_store_list_match['address']
output_tenpo_not_match_pw['access'] = PW_store_list_match['access']
output_tenpo_not_match_pw['closeday'] = PW_store_list_match['closeday']
output_tenpo_not_match_pw['opentime'] = PW_store_list_match['opentime']
output_tenpo_not_match_pw['service'] = PW_store_list_match['service']
output_tenpo_not_match_pw['rate'] = PW_store_list_match['rate']
output_tenpo_not_match_pw['dai'] = PW_store_list_match['dai']
output_tenpo_not_match_pw['dai_p'] = PW_store_list_match['dai_p']
output_tenpo_not_match_pw['dai_s'] = PW_store_list_match['dai_s']
output_tenpo_not_match_pw['dai_sum'] = PW_store_list_match['dai_sum']
output_tenpo_not_match_pw['parking'] = PW_store_list_match['parking']
output_tenpo_not_match_pw['tel'] = PW_store_list_match['tel']
output_tenpo_not_match_pw['tenpo_update'] = PW_store_list_match['tenpo_update']
output_tenpo_not_match_pw['co_date'] = PW_store_list_match['co_date']
output_tenpo_not_match_pw['merge_url'] = PW_store_list_match['url']
output_tenpo_not_match_pw['pw_t_code'] = PW_store_list_match['pw_t_code']

output_tenpo_not_match_dm['state_cd'] = DM_store_list_match['state_cd']
output_tenpo_not_match_dm['t_code'] = DM_store_list_match['t_code']
output_tenpo_not_match_dm['ホール名'] = DM_store_list_match['name']
output_tenpo_not_match_dm['address'] = DM_store_list_match['address']
output_tenpo_not_match_dm['access'] = DM_store_list_match['access']
output_tenpo_not_match_dm['closeday'] = DM_store_list_match['closeday']
output_tenpo_not_match_dm['opentime'] = DM_store_list_match['opentime']
output_tenpo_not_match_dm['service'] = DM_store_list_match['service']
output_tenpo_not_match_dm['rate'] = DM_store_list_match['rate']
output_tenpo_not_match_dm['dai'] = DM_store_list_match['dai']
output_tenpo_not_match_dm['dai_p'] = DM_store_list_match['dai_p']
output_tenpo_not_match_dm['dai_s'] = DM_store_list_match['dai_s']
output_tenpo_not_match_dm['dai_sum'] = DM_store_list_match['dai_sum']
output_tenpo_not_match_dm['parking'] = DM_store_list_match['parking']
output_tenpo_not_match_dm['tel'] = DM_store_list_match['tel']
output_tenpo_not_match_dm['co_date'] = DM_store_list_match['co_date']
output_tenpo_not_match_dm['merge_url'] = DM_store_list_match['url']
output_tenpo_not_match_dm['tenpo_update'] = DM_store_list_match['tenpo_update']
output_tenpo_not_match_dm['dmm_t_code'] = DM_store_list_match['dmm_t_code']

logging.info('==============Connect: Daito-Network==================')

# SFTP接続先の設定
HOST = 'sftp.daitogiken.com'
PORT = 2022
SFTP_USER = 'kwk-001'
SFTP_PASSWORD = 'EtrWJPf5'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
client.connect(HOST, port=PORT, username=SFTP_USER, password=SFTP_PASSWORD)
try:
    # SFTPセッション開始
    sftp_connection = client.open_sftp()
    # ファイル出力
    file_kisyu = sftp_connection.file('/p_kisyu_'+ dir_date +'.csv', "a", -1)
    file_kisyu.write("\uFEFF")
    file_kisyu.write(output_kisyu_to_csv.to_csv(index=False, encoding="utf-8"))
    file_dai = sftp_connection.file('/p_dai_'+ dir_date +'.csv', "a", -1)
    file_dai.write("\uFEFF")
    file_dai.write(output_dai_to_csv.to_csv(index=False, encoding="utf-8"))
    file_tempo = sftp_connection.file('/p_tenpo_'+ dir_date +'.csv', "a", -1)
    file_tempo.write("\uFEFF")
    file_tempo.write(output_tenpo_to_csv.to_csv(index=False, encoding="utf-8"))
finally:
    client.close()

# SFTP接続先の設定
HOST = '104.198.89.41'
PORT = 'ssh'
USER = 'y_iwano'
KEY_FILE  = './id_rsa'
# 秘密鍵ファイルからキーを取得
rsa_key = paramiko.RSAKey.from_private_key_file(KEY_FILE)
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    #SFTPセッション開始
    client.connect(HOST, PORT, USER, pkey=rsa_key) # キーを指定することでパスワードは必要なし
    sftp_connection = client.open_sftp()
    # ファイル出力
    file_kisyu1 = sftp_connection.file('/home/y_iwano/Daito/NMatch/kisyu_PW_'+ dir_date +'.csv', "a", -1)
    file_kisyu1.write("\uFEFF")
    file_kisyu1.write(output_kisyu_not_match_PW.to_csv(index=False, encoding="utf-8"))
    file_kisyu2 = sftp_connection.file('/home/y_iwano/Daito/NMatch/NMatch/kisyu_DM_'+ dir_date +'.csv', "a", -1)
    file_kisyu2.write("\uFEFF")
    file_kisyu2.write(output_kisyu_not_match_DM.to_csv(index=False, encoding="utf-8"))
    file_tempo1 = sftp_connection.file('/home/y_iwano/Daito/NMatch/NMatch/tenpo_DM_'+ dir_date +'.csv', "a", -1)
    file_tempo1.write("\uFEFF")
    file_tempo1.write(output_tenpo_not_match_pw.to_csv(index=False, encoding="utf-8"))
    file_tempo2 = sftp_connection.file('/home/y_iwano/Daito/NMatch/NMatch/tenpo_PW_'+ dir_date +'.csv', "a", -1)
    file_tempo2.write("\uFEFF")
    file_tempo2.write(output_tenpo_not_match_dm.to_csv(index=False, encoding="utf-8"))
    file_pair_kisyu = sftp_connection.file('/home/y_iwano/Daito/Pair_data/pair_kisyu.csv', "a", -1)
    file_pair_kisyu.write("\uFEFF")
    file_pair_kisyu.write(output_pair_to_csv.to_csv(index=False, encoding="utf-8"))
    file_pair_tenpo = sftp_connection.file('/home/y_iwano/Daito/Pair_data/pair_tenpo.csv', "a", -1)
    file_pair_tenpo.write("\uFEFF")
    file_pair_tenpo.write(output_pair.to_csv(index=False, encoding="utf-8"))
finally:
    client.close()

logging.info('==========================end=========================')