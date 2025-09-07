from libs.database.connection import DatabaseConnection
import json

def main():
    db = DatabaseConnection()
    db.create_database()

    news_id = 257171

    out_dict = db.get_news_and_infos_for_ai(news_id)
    
    # news = db.get_news_by_id(news_id)
    # # news_list = db.get_news_by_symbol(symbol="AAPL", limit=1)
    # out_dict: dict[str, dict] = {}
    # news_dict = dict(news)
    # out_dict['news'] = news_dict

    # symbols = json.loads(news['symbols_json'])
    # symbol_info_dict: dict[str, dict] = {}
    # for symbol in symbols:
    #     symbol_info_dict[symbol] = db.get_infos(symbol)
    # out_dict['symbol_info'] = symbol_info_dict
    pass

    # news_list = db.get_all_news()
    # print(news_list)

if __name__ == "__main__":
    main()