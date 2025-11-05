
from apps.ai.pipelines import news_analyzer_2
from libs.database.connection import DatabaseConnection
from pathlib import Path
import json
from typing import Any, Optional, get_origin
from datetime import datetime, timezone
from apps.ai.perform_stage_b_entity_alias_formation import normalize_name
import time
import pandas as pd
from apps.ingest.web_search.search_manager import WebSearchManager


def get_news_context(news_id: int):
    db = DatabaseConnection("data/db/news.db")

    # Initialize manager
    manager = WebSearchManager(db)
    # Check backoff status
    manager.check_backoff_status(silence=True)

    news_analysis_a = db.get_news_analysis_a(news_id=news_id)

    # Extract symbols information
    symbols_description = dict[str,dict]()
    symbols_fundamentals = dict[str,dict]()
    for symbol in news_analysis_a['symbols_input']:
        # Search fundamentals
        fundamentals = db.get_fundamentals(symbol, remove_none_fields=True)
        if fundamentals:
            symbols_fundamentals[symbol] = fundamentals
        # Search symbol info (entity info)
        res = db.find_entity_by_symbol(symbol)
        if res and len(res) > 0:
            symbols_description[symbol] = {
                key: value for key, value in res.items() 
                if value is not None and not key in ('zip', 'phone', 'address1', 'confidence', 'created_at')
                }
            symbols_description[symbol]['source_type'] = 'infos'
        else:
            # Internet search
            res = manager.search(query=symbol, fuzzy=False, entity_type='symbol')
            if res.get('status', None) in {'ok'} and res.get('results', None):
                if not res['cached']:
                    print(f"Performed web-search of symbol '{symbol}'")
                symbols_description[symbol] = {i: result for i, result in enumerate(res['results'])}
                symbols_description[symbol]['source_type'] = 'web search'
            else:
                res = manager.search(query=symbol, fuzzy=True)
                print(f"Performed fuzzy search '{symbol}'")
                symbols_description[symbol] = {i: result for i, result in enumerate(res['results'])}
                symbols_description[symbol]['source_type'] = 'web search (fuzzy)'

    # Extract actors information
    actors_description = dict[str, dict]()
    for actor in news_analysis_a['actors']:
        # Try exact alias match (no fuzzy)
        res = db.find_entity_by_alias(actor['name'], fuzzy=False)
        if res and len(res) > 0 and isinstance(res[0], dict) and 'entity' in res[0] and isinstance(res[0]['entity'], dict):
            actors_description[actor['name']] = {
                key: value
                for key, value in res[0]['entity'].items()
                if value is not None and not key in ('zip', 'phone', 'address1', 'confidence', 'created_at')
            }
            actors_description[actor['name']]['source_type'] = 'alias'
        else:
            # First, if actor is a person, try normalized person search by name
            if actor.get('type') == 'person':
                normalize_named = normalize_name(actor['name'])
                res = db.find_person_by_name(
                    normalize_named.family_norm,
                    normalize_named.given_norm,
                    normalize_named.given_prefix3
                )
                if res and len(res) > 0 and isinstance(res[0], dict):
                    actors_description[actor['name']] = {
                        key: value
                        for key, value in res[0].items()
                        if value is not None
                    }
                    actors_description[actor['name']]['source_type'] = 'infos (person)'
                    continue                   

            # If not found as person, try fuzzy alias match
            res = db.find_entity_by_alias(actor['name'], fuzzy=True)
            if res and len(res) > 0:
                # Create a dictionary {entity_id: entity} for automatic deduplication
                actors_description[actor['name']] = {
                    item['entity']['entity_id']: {k:v for k,v in item['entity'].items() if v is not None}
                    for item in res
                    if isinstance(item, dict) 
                    and 'entity' in item 
                    and isinstance(item['entity'], dict)
                    and item['entity'].get('entity_id') is not None
                }
                actors_description[actor['name']]['source_type'] = 'infos (fuzzy)'
                continue
            # Last resort: web/internet (cached) search
            res = manager.search(query=actor['name'], fuzzy=False, entity_type=actor.get('type'))
            if res.get('status', None) in {'ok'} and res.get('results', None):
                if not res.get('cached', True):
                    print(f"Performed web-search of actor '{actor['name']}'")
                actors_description[actor['name']] = {i: result for i, result in enumerate(res['results'])}
                actors_description[actor['name']]['source_type'] = 'web search'
            else:
                # Optionally, could try fuzzy web search as last fallback
                res = manager.search(query=actor['name'], fuzzy=True, entity_type=actor.get('type'))
                if res.get('status', None) in {'ok'} and res.get('results', None):
                    if not res.get('cached', True):
                        print(f"Performed fuzzy web-search of actor '{actor['name']}'")
                    actors_description[actor['name']] = {i: result for i, result in enumerate(res['results'])}
                    actors_description[actor['name']]['source_type'] = 'web search (fuzzy)'
                else:
                    actors_description[actor['name']] = {}
                    actors_description[actor['name']]['source_type'] = 'not found'

    # Affilations
    affilations_description = dict[str, dict]()
    for actor, entity_dict in actors_description.items():
        if entity_dict.get("entity_type", "not person") in ("person"):
            canonical_full_name = entity_dict['canonical_full']
            affilations_description[canonical_full_name] = dict()
            person_id = entity_dict['entity_id']
            affiliations: list[dict] = db.find_person_affiliations(person_id)
            if affiliations:
                for i_affilation, affilation in enumerate(affiliations):
                    affilation['org'] = {k:v for k,v in affilation['org'].items() if v is not None and not k in ('zip', 'phone', 'address1', 'confidence', 'created_at')} 
                    affilation = {k:v for k,v in affilation.items() if v is not None and not k in ('confidence')}
                    affilations_description[canonical_full_name][i_affilation] = affilation
                pass
            print(f"Found affilations for {canonical_full_name} (#{len(affilations_description)})")


    combined_context = dict[str, dict]()
    combined_context['news_analysis_a'] = news_analysis_a
    combined_context['symbols_description'] = symbols_description
    combined_context['symbols_fundamentals'] = symbols_fundamentals
    combined_context['actors_description'] = actors_description
    combined_context['affilations_description'] = affilations_description

    return combined_context
    


if __name__ == "__main__":
    db = DatabaseConnection("data/db/news.db")
    
    id_to_len = dict[int,int]()
    total_news_analysis_a = db.get_total_news_analysis_a()
    for i_news_analysis_a, news_analysis_a in enumerate(db.iterate_news_analysis_a(), start=1):
        news_id = news_analysis_a['news_id']
        news_context = get_news_context(news_id)
        context_len = len(str(news_context))
        id_to_len[news_id] = context_len
        
        import time

        # Start timing outside loop if not already done
        if 'start_time_total' not in locals():
            start_time_total = time.time()

        elapsed_seconds = time.time() - start_time_total
        if i_news_analysis_a > 0:
            avg_time = elapsed_seconds / i_news_analysis_a
            est_total = avg_time * total_news_analysis_a
            rem_seconds = est_total - elapsed_seconds
        else:
            rem_seconds = 0

        # Format min:sec
        def minsec(seconds):
            m = int(seconds // 60)
            s = int(seconds % 60)
            return f"{m:02d}:{s:02d}"

        elapsed_str = minsec(elapsed_seconds)
        rem_str = minsec(max(rem_seconds, 0))

        line_str = f"[news_id:{news_id}|{i_news_analysis_a}/{total_news_analysis_a}|{i_news_analysis_a/total_news_analysis_a*100:.2f}%|run_time {elapsed_str}|eta {rem_str}|] symbol length = {context_len}" 
        print("="*len(line_str))
        print(line_str)

        print("  Context Length Summary  ".center(len(line_str),"="))
        print(f"Total news: {len(id_to_len)}")
        if id_to_len:
            lengths = sorted(id_to_len.items(), key=lambda x: x[1])
            values = [v for k,v in lengths]
            import statistics
            avg_len = sum(values)/len(values)
            median_len = statistics.median(values)
            min_len = values[0]
            min_id = lengths[0][0]
            max_len = values[-1]
            max_id = lengths[-1][0]
            print(f"Average length: {avg_len:.2f}")
            print(f"Median length: {median_len:.2f}")
            print(f"Minimum length: {min_len} (news_id: {min_id})")
            print(f"Maximum length: {max_len} (news_id: {max_id})")
            # Percentiles
            def percentile(p):
                if not values: return None
                k = max(0, min(len(values)-1, int(len(values)*p/100)))
                return values[k], lengths[k][0]
            print()
            print("Percentile values of context length:")
            for perc in [0,10,25,50,75,90,95,99,100]:
                idx = int((len(values)-1) * perc / 100)
                val = values[idx]
                val_id = lengths[idx][0]
                print(f"{perc:>4}%: length = {val}, news_id = {val_id}")
            # Count in each interval:
            boundaries = [0,10,25,50,75,90,95,99,100]
            counts = []
            for i in range(len(boundaries)-1):
                lo = int((len(values))*boundaries[i]/100)
                hi = int((len(values))*boundaries[i+1]/100)
                counts.append(hi-lo)
            print()
            print("Number of news in each percentile interval (in percent):")
            # for i, c in enumerate(counts):
            #     print(f"{boundaries[i]:>4}% - {boundaries[i+1]:>3}%: {c} news ({c/len(values)*100:.2f}%)")

            for i, c in enumerate(counts):
                n_stars = int(c/len(values)*100)
                n_spaces = 100 - n_stars
                print(f"{boundaries[i]:>4}% - {boundaries[i+1]:>3}%: |{'*'*n_stars}{' '*n_spaces}| {c} news ({c/len(values)*100:.2f}%)")
