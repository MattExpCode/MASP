# -- coding: utf-8 --
import json
from time import sleep

import pandas as pd
import pymongo
import re
import ast

from Mongo_utilze import save2db


def create_mongodb():
    # filename = "D:\\LAB\\Demo\\JAVA\\Tmcatest\\src\\main\\java\\Youtube_UserProfileResult.txt"
    filename = "D:\\LAB\\Demo\\EXTEND_DATA\\FILES\\Youtube_UserProfileResult_EX.txt"
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["Youtube"]
    mycol = mydb['UserID_EX_all']
    print(mycol.count_documents({}))
    with open(filename, 'r+') as f:
        while True:
            line = f.readline()
            if not line:
                break
            parts = line.split(' ')
            info = {'userid': parts[1], 'userOid': parts[3]}
            # ==Warning!== # mycol.delete_many({})
            mycol.update_one({'userid': info['userid']}, {'$set': info}, True)
            print(info)


# <videoid> id
# <videoOrid> bus_id </videoOrid>
# <author> name </author>
# <authorid> bus_id </authorid>
# <date> 0 </date>
# <keywords> video, sharing, camera phone, video phone, free, upload </keywords>
# <title> name  </title>
# <userlist> UCjHGkSGTAOhcKRD3UK88D1g UCfcJa6ltBBtV97dAELPZkMw UCca5ZF9J5zbjGpuElBYbKUQ UCh1_Zk1uit2kZFw21i-2U6g UC3BY4VVdbtB0973DYwMCvRg UCyYEpvsmDAALi8B8Wv7ku2A </userlist>
# <description> categories

def Generate_Video_Info(item):
    video_id = item['id']
    video_Oid = item['business_id']
    author_name = item['name']
    channel_id = item['business_id']
    date = '0'
    keywords = item['categories']
    title = item['name']
    user_list = []
    if "user_list" in item:
        user_list = item['user_list']
        # print(item['id'], ': ', len(user_list))
    else:
        print(item['id'], ': ', 0)
    despription = item['categories']

    Info = {'id': video_Oid, 'author': author_name, 'channel_id': channel_id, 'title': title,
            'description': despription, 'date': date, 'kwords': keywords, 'user_list': user_list}
    return Info
    #
    #     br[0] = '<videoid> ' + str(j) + ' </videoid>'
    #     br[1] = '<videoOrid> ' + single_item['id'] + ' </videoOrid>'
    #     br[2] = '<author> ' + cleantxt(single_item['author']) + ' </author>'
    #     br[3] = '<authorid> ' + single_item['channel_id'] + ' </authorid>'
    #     # br[4] = '<date> ' + single_item['date'] + ' </date>'
    #     time_range = time_redu(single_item['date'] + ' ')
    #     br[4] = '<date> ' + str(time_range.days) + ' </date>'
    #     br[5] = '<keywords> ' + cleantxt(single_item['kwords']) + ' </keywords>'
    #     check_str_1 = cleantxt(single_item['title'])
    #     if check_str_1 == '':
    #         flag_1 = 0
    #     br[6] = '<title> ' + check_str_1 + ' </title>'
    #     tmpstr = list2string(single_item['user_list'])
    #     br[7] = '<userlist> ' + tmpstr + ' </userlist>'
    #     check_str_2 = cleantxt(single_item['description'])
    #     if check_str_2 == '':
    #         flag_2 = 0
    #     br[8] = '<description> ' + cleantxt(single_item['description']) + ' </description>'
    #
    #     # break
    # return Buffer_Video


def update_item_col():
    path_ = 'D:\\LAB\\Demo\\EXTEND_DATA\\yelp\\jsons\\'
    filename = path_ + 'yelp_academic_dataset_business.json'
    count = 0
    th = 49999
    col_name = 'yelp_item'
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            if not line:
                break
            else:
                if count < th:
                    count += 1
                    continue
                info = json.loads(line)
                info.update({"id": count})
                # print(type(info))
                # print(info.keys())
                # print(info.values())
                # save2db.save_info(col_name, info)
                count += 1
                # break
                if count % 50 == 0:
                    print(count)
                    print(info.values())
                # if count == 50000:
                #     break
    save2db.print_col_info(col_name)
    print('total number: ', count)


def list2string(list_):
    str = []
    # print(type(list_))
    for i in range(0, len(list_)):
        str.append(list_[i])
    str1 = ' '.join(str)
    # str1 = '<userlist> ['+str1+'] </userlist>'
    # print((str1))
    return str1


def cleantxt_(raw):
    fil = re.compile(u'[^0-9a-zA-Z\u4e00-\u9fa5.，,。？“”]+', re.UNICODE)
    return fil.sub(' ', raw)


def cleantxt(raw):
    return raw


def write_down(dict_list, filename, offset):
    br = [''] * 9
    with open(filename, 'w+', encoding="utf-8") as f:
        for j in range(len(dict_list)):
            if j % 10 == 0:
                print(j)
            flag_1 = 1
            flag_2 = 1
            single_item = dict_list[j]
            br[0] = '<videoid> ' + str(j + offset) + ' </videoid>'
            br[1] = '<videoOrid> ' + single_item['id'] + ' </videoOrid>'
            br[2] = '<author> ' + cleantxt(single_item['author']) + ' </author>'
            br[3] = '<authorid> ' + single_item['channel_id'] + ' </authorid>'
            br[4] = '<date> ' + single_item['date'] + ' </date>'
            if not single_item['kwords']:
                br[5] = '<keywords> ' + (single_item['author']) + ' </keywords>'
                br[8] = '<description> ' + (single_item['author']) + ' </description>'
            else:
                br[5] = '<keywords> ' + (single_item['kwords']) + ' </keywords>'
                br[8] = '<description> ' + (single_item['description']) + ' </description>'
            br[6] = '<title> ' + cleantxt(single_item['title']) + ' </title>'
            tmpstr = list2string(single_item['user_list'])
            br[7] = '<userlist> ' + tmpstr + ' </userlist>'
            for b in br:
                f.write(b + "\n")
                # print(b)


def gene_info_file():
    col_name = 'yelp_item'
    col_item = save2db.print_col_info(col_name)
    Buffer_Item = []
    FN_List = []
    count = 0
    Tcount = 0
    FileSize = 2000
    for ele in col_item:
        new_ele = Generate_Video_Info(ele)
        # if "user_list" in ele:
        #     uL = ele['user_list']
        #     # uL = ele['user_list']
        #     print(ele['id'], ': ', len(uL))
        # else:
        #     # print('no user now')
        #     print(ele['id'], ': ', 0)
        # print(new_ele.keys())
        # print(new_ele.values())
        Buffer_Item.append(new_ele)
        count += 1
        if count % FileSize == 0:
            Tcount = count // FileSize
            filename = 'Mongo_utilze\\data4\\Yelp_Info_' + str(Tcount) + '.txt'
            write_down(Buffer_Item, filename, (Tcount - 1) * FileSize)
            FN_List.append('Yelp_Info_' + str(Tcount) + '\n')
            Buffer_Item = []
        # if count == 50000:
        #     break
    if len(Buffer_Item) > 0:
        Tcount += 1
        write_down(Buffer_Item, 'Mongo_utilze\\data3\\Yelp_Info_' + str(Tcount) + '.txt', (Tcount - 1) * FileSize)
        FN_List.append('Yelp_Info_' + str(Tcount) + '\n')
    with open('Mongo_utilze\\YoutubeFileList_new.txt', 'w+', encoding="utf-8") as f:
        for file in FN_List:
            f.write(file)


def find_userlist():
    path_ = 'D:\\LAB\\Demo\\EXTEND_DATA\\yelp\\jsons\\'
    filename = path_ + 'yelp_academic_dataset_review.json'
    count = 0
    th = 6921055  # 4854164/96416
    col_name = 'yelp_item'
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            if not line:
                break
            else:
                if count < th:
                    count += 1
                    continue
                print("handing ", count)
                info = json.loads(line)
                # print(type(info['stars']))
                star = info['stars']
                if star < 3:
                    count += 1
                    print("not satisfied!")
                    continue
                bid = info['business_id']
                query = {}
                query.update({"key_name": 'business_id'})
                query.update({"key_value": bid})
                old_info = save2db.search_info(col_name, query)
                if not old_info:
                    count += 1
                    print("no such item!")
                    continue

                # old_info.update({'user_list': ['awdsadwa']})
                # list_ = info['user_list']
                if "user_list" in old_info:
                    uL = old_info['user_list']
                else:
                    print('no user now')
                    uL = []

                if info['user_id'] in uL:
                    count += 1
                    continue
                uL.append(info['user_id'])
                old_info.update({'user_list': uL})
                # print(old_info.keys())
                # print(old_info.values())
                print(count, len(uL))
                save2db.save_info(col_name, old_info)
                count += 1
                # if count % 100 == 0:
                #     print(count, len(uL))
                # if count >= 0:
                #     break
    col_item = save2db.print_col_info(col_name)
    for ele in col_item:
        if "user_list" in ele:
            uL = ele['user_list']
            # uL = ele['user_list']
            print(ele['id'], ': ', len(uL))
        else:
            # print('no user now')
            print(ele['id'], ': ', 0)
        print(ele.keys())
        print(ele.values())
        break
    # print(ele.keys())
    # break


def create_userprofile():
    col_item = save2db.print_col_info('yelp_item')
    col_user = save2db.print_col_info('yelp_user')
    # myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    # mydb = myclient["Youtube"]
    # mycol = mydb['yelp_user']
    # mycol.drop()
    TH = 147907  # 95085
    for_count = 0
    for ele in col_item:
        if for_count < TH:
            for_count += 1
            continue
        # print(ele.keys())
        # print(ele.values())
        print('item id: ', ele['id'])
        # print(ele['business_id'])
        if 'user_list' in ele:
            uL = ele['user_list']
        else:
            uL = []
            ele.update({'user_list': uL})
            save2db.save_info('yelp_item', ele)
        bid = ele['business_id']
        print('User list: ', ele['user_list'])

        for userOid in uL:
            query = {}
            query.update({"key_name": 'user_Oid'})
            query.update({"key_value": userOid})
            try:
                user_info = save2db.search_info('yelp_user', query)
            except:
                user_info = save2db.search_info('yelp_user', query)
                sleep(0.1)
            if not user_info:
                col_user = save2db.print_col_info('yelp_user', 0)
                iL = [bid]
                user_info = {'user_Oid': userOid, 'id': col_user.count(), 'item_list': iL, 'loc': ele['state']}
                print('create new up No.', user_info['id'])
                save2db.save_info('yelp_user', user_info)
            else:
                iL = user_info['item_list']
                if bid not in iL:
                    iL.append(bid)
                    user_info.update({'item_list': iL})
                    save2db.save_info('yelp_user', user_info)
                    print('find the user and update her item list.')
                else:
                    # print(bid)
                    # print(iL)
                    print('duplicated historical item!')
        for_count += 1
        # if for_count == 10:
        #     break
    col_user = save2db.print_col_info('yelp_user')
    # for ele in col_user:
    #     print(ele)


def static_data():
    col_item = save2db.print_col_info('yelp_item')
    # col_user = save2db.print_col_info('yelp_user')
    count = 0
    T_count = 0
    th = 23013
    for item in col_item:
        name = item['name']
        attr = item['attributes']
        user_list = item['user_list']
        # print(item)

        if 'name' in item:
            nL = len(item['name'])
        else:
            nL = 0
            print(T_count, 'name? None!')

        # print('---------------')
        # print(item)
        # print(item['attributes'])
        if 'attributes' in item:
            if item['attributes'] != None:
                aL = len(item['attributes'])
            else:
                print(T_count, 'shit_None!')
                aL = 0
        else:
            aL = 0

        if 'user_list' in item:
            uL = len(item['user_list'])
        else:
            uL = 0
            print(T_count, 'name? None!')

        if nL == 0 or aL == 0 or uL == 0:
            count += 1
        # print(unL)
        # print(len(attr))
        # print(len(user_list))
        T_count += 1
        if T_count == th:
            break
    ratio = count / T_count
    print('ratio: ', ratio)


def FromStr2Dict(strline):
    # print('string now: ', strline)
    result = strline
    if isinstance(strline, str):
        if strline.startswith('{') and strline.endswith('}') and ':' in strline:
            try:
                result = ast.literal_eval(strline)
                if isinstance(result, dict):
                    # print('Yes-----')
                    pass
                else:
                    # print(' ')
                    pass
            except (ValueError, SyntaxError):
                print('fucked up')
    return result


def generate_attr(attributes):
    print('current dict: ', attributes.keys())
    ATTs = []

    # print(attributes['BusinessParking'])
    # print(type(attributes['BusinessParking']))
    #
    #

    for key in attributes.keys():
        res = FromStr2Dict(attributes[key])
        if isinstance(res, dict):
            print('================')
            # print(res)
            subATTs = generate_attr(res)
            for sub_key in subATTs:
                attr_str = key + '_' + sub_key
                ATTs.append(attr_str)
        else:
            attr_str = key + '_' + str(attributes[key])
            # print(attr_str)
            ATTs.append(attr_str)
    return ATTs


def extract_attribute():
    SaveFile = True
    col_item = save2db.print_col_info('yelp_item')
    item = None
    Count = 0
    th = 3794
    Item_Attribute_List = []
    for item_cur in col_item:
        # if Count != th:
        #     Count += 1
        #     continue
        item = item_cur
        # break
        attr = item['attributes']
        if attr is None:
            list_attr = []
        else:
            list_attr = generate_attr(attr)
        print(Count, attr)
        print(list_attr)
        Item_Attribute_List.append(list_attr)
        Count += 1
    if SaveFile:
        filepath = "ItemAttributeList.json"
        with open(filepath, "w") as file:
            json.dump(Item_Attribute_List, file)


def AttributeAnalysis():
    filepath = "ItemAttributeList.json"
    with open(filepath, "r") as file:
        load_list = json.load(file)
    unique_strings = set()
    string_counts = {}

    Tcount = 0
    # 遍历每个列表
    for lst in load_list:
        Tcount += 1
        for string in lst:
            # 添加到 set
            unique_strings.add(string)

            # 统计次数
            if string in string_counts:
                string_counts[string] += 1
            else:
                string_counts[string] = 1
        # if Tcount == 23000:
        #     break
    # 输出结果
    print("Unique strings:", unique_strings)
    print("String counts:", string_counts)
    print(len(unique_strings))
    file_path = "string_counts.json"  # 文件路径

    with open(file_path, "w") as file:
        json.dump(string_counts, file)
        print(f"String counts saved to {file_path}")


def threshold_analysis(numbers, th):
    count_less_equal_t = sum(1 for num in numbers if num <= th)

    # 计算占比
    total_count = len(numbers)
    percentage = (count_less_equal_t / total_count) if total_count > 0 else 0

    return percentage


def ImprotanceAnalysis():
    # import json

    filepath = "ItemAttributeList.json"
    with open(filepath, "r") as file:
        lists = json.load(file)

    # 载入 string_counts
    string_counts_file = "string_counts.json"
    with open(string_counts_file, "r") as file:
        string_counts = json.load(file)

    # 计算总共有多少个列表 (n)
    n = len(lists)

    # 初始化结果字典
    string_statistics = {}

    # 遍历每个字符串，计算其频率及统计信息
    for string, count in string_counts.items():
        # 计算频率
        frequency = count / n
        # 找出包含该字符串的列表的长度
        lengths = []
        for lst in lists:
            if string in lst:
                lengths.append(len(lst))

        # 计算最大长度、最小长度、平均长度
        if lengths:
            max_length = max(lengths)
            min_length = min(lengths)
            avg_length = sum(lengths) / len(lengths)
        else:
            max_length = min_length = avg_length = 0  # 如果没有列表包含该字符串，长度设为0
        th_analysis = []
        for delta in range(5):
            th = min_length + delta
            perc = threshold_analysis(lengths, th)
            th_analysis.append(perc)

        # 保存结果
        string_statistics[string] = {
            "frequency": frequency,
            "max_length": max_length,
            "min_length": min_length,
            "avg_length": avg_length,
            "reverse_importance": lengths,
            "th_analysis": th_analysis
        }

    # 输出结果
    # for string, stats in string_statistics.items():
    #     print(f"String: {string}")
    #     print(f"  Frequency: {stats['frequency']:.4f}")
    #     print(f"  Max length: {stats['max_length']}")
    #     print(f"  Min length: {stats['min_length']}")
    #     print(f"  Avg length: {stats['avg_length']:.2f}")
    #     print("-" * 30)
    file_path = "string_res.json"  # 文件路径

    with open(file_path, "w") as file:
        json.dump(string_statistics, file)
        print(f"String counts saved to {file_path}")
    print('================================================')
    sorted_statistics = sorted(string_statistics.items(), key=lambda item: item[1]['frequency'], reverse=True)
    # 计算后 30% 的索引范围
    total_count = len(sorted_statistics)
    cutoff_index = int(total_count * 0.0)

    # 获取后 30% 的结果
    back_30_percent = sorted_statistics[cutoff_index:]
    # for string, stats in back_30_percent:
    #     print(f"Attribute: {string}")
    #     print(f"  Frequency: {stats['frequency']:.6f}")
    #     print(f"  Max length: {stats['max_length']}")
    #     print(f"  Min length: {stats['min_length']}")
    #     print(f"  Avg length: {stats['avg_length']:.2f}")
    #     print("Reverse importance: ", stats['reverse_importance'])
    #     print("Threshold Analysis", stats['th_analysis'])
    #     print("-" * 30)
    output_file = "res.txt"

    with open(output_file, "w") as file:
        for string, stats in back_30_percent:
            file.write(f"String: {string}\n")
            file.write(f"  Frequency: {stats['frequency']:.6}\n")
            file.write(f"  Max length: {stats['max_length']}\n")
            file.write(f"  Min length: {stats['min_length']}\n")
            file.write(f"  Avg length: {stats['avg_length']:.2f}\n")
            file.write(f"  Threshold Analysis:: {stats['th_analysis']}\n")
            file.write(f"  Reverse importance: {stats['reverse_importance']}\n")
            file.write("-" * 30 + "\n")

    print(f"Results saved to {output_file}")


def filter_list():
    import json

    # 示例数据
    filepath = "ItemAttributeList.json"
    with open(filepath, "r") as file:
        lists = json.load(file)

    # 载入 string_counts
    string_counts_file = "string_counts.json"
    with open(string_counts_file, "r") as file:
        string_counts = json.load(file)
    # 总列表数量
    n = len(lists)

    # 计算每个字符串的频率
    string_frequencies = {string: count / n for string, count in string_counts.items()}

    # 筛选条件
    k = 6  # 定义 k 值
    frequency_threshold = 0.0003  # 定义频率阈值

    # 筛选出长度小于 k 且包含频率小于指定值的字符串的列表
    filtered_lists = [
        lst for lst in lists
        if len(lst) < k and any(string_frequencies.get(string, 0) < frequency_threshold for string in lst)
    ]

    # 统计结果
    filtered_count = len(filtered_lists)
    print('number: ', filtered_count)
    total_count = len(lists)
    proportion = (filtered_count / total_count) * 100 if total_count > 0 else 0

    # 输出结果
    print(f"Number of lists meeting criteria: {filtered_count}")
    print(f"Proportion of such lists: {proportion:.2f}%")


if __name__ == '__main__':
    # ===========create collection to store the Yelp business info
    # update_item_col()
    # ===========fulfil the user list of each item
    # find_userlist()
    # ============generate _Info file=========
    # gene_info_file()
    # ============create user profile============
    # create_userprofile()
    # =============statstic===============
    # static_data()
    # =============extract attr============
    # extract_attribute()
    # analyse attributes
    # AttributeAnalysis()
    # analyse attributes importancec
    # ImprotanceAnalysis()

    filter_list()
