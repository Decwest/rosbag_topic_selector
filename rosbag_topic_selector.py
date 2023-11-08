import rosbag
from collections import defaultdict
from io import BytesIO
from tqdm import tqdm

def analyze_rosbag_topics_size(rosbag_path):
    topic_sizes = defaultdict(int)
    
    with rosbag.Bag(rosbag_path, 'r') as bag:
        for topic, msg, t in tqdm(bag.read_messages(), total=bag.get_message_count(), desc="load image and tf"):
            # バッファを生成
            buff = BytesIO()
            
            # メッセージをバッファにシリアライズ
            msg.serialize(buff)
            
            # バッファの現在の位置（サイズ）を取得
            topic_sizes[topic] += buff.tell()

    # サイズをギガバイトに変換
    topic_sizes_gb = {topic: size / (1024 ** 3) for topic, size in topic_sizes.items()}
    total_size_gb = sum(topic_sizes_gb.values())

    # サイズに基づいて辞書の項目を並び替え、大きい順にする
    sorted_topics = sorted(topic_sizes_gb.items(), key=lambda item: item[1], reverse=True)

    # 並び替えた結果を出力
    for topic, size_gb in sorted_topics:
        print(f"Topic: {topic}, Size: {size_gb:.3f} GB")
        
    # 合計サイズを出力
    print(f"Total Size: {total_size_gb:.3f} GB")


# 実際にはユーザーが指定するrosbagファイルパス
rosbag_file_path = '/home/ytpc2022e/decwest_workspace/python-test/_2023-11-08-19-33-51.bag'
analyze_rosbag_topics_size(rosbag_file_path)
