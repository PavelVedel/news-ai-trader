"""
Stage B: Entity Alias Formation and Name Normalization

This module handles the normalization of entity names, particularly person names,
to create standardized derivatives for matching and grounding purposes.
"""

import re
import unicodedata
import json
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path
import sys

# Add libs to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from libs.database.connection import DatabaseConnection


@dataclass
class NameNormalizationResult:
    """
    Result of name normalization containing all derived fields for matching (for persons only).
    
    Attributes:
        family_norm: Normalized family name (lowercase, NFKD, no diacritics)
        given_norm: Normalized given name (lowercase, NFKD, no diacritics)  
        given_initial: First letter of given name (lowercase)
        given_prefix3: First 2-3 characters of given name (lowercase)
        middle_initials: Middle name initials without dots (lowercase)
        full_norm_no_honor: Full name without titles, normalized
    """
    family_norm: Optional[str] = None
    given_norm: Optional[str] = None
    given_initial: Optional[str] = None
    given_prefix3: Optional[str] = None
    middle_initials: Optional[str] = None
    full_norm_no_honor: Optional[str] = None


def normalize_name(full_name: str) -> NameNormalizationResult:
    """
    Normalize a person's full name and extract derivatives for matching.
    
    Input: Full name string (e.g., "Mr. Timothy D. Cook")
    Output: NameNormalizationResult with normalized derivatives
    
    Args:
        full_name: The full name string to normalize
        
    Returns:
        NameNormalizationResult containing all normalized name derivatives
    """
    
    # Initialize result object
    result = NameNormalizationResult()
    
    if not full_name or not isinstance(full_name, str):
        return result
    
    # Clean and normalize the input
    name = full_name.strip()
    if not name:
        return result
    
    # Remove common titles/honorifics AND academic degrees/suffixes
    titles = [
        r'\b(Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?|Professor|Sir|Dame|Lord|Lady)\b',
        r'\b(CEO|CTO|CFO|COO|President|Chairman|Chairwoman|Director)\b',
        r'\b(General|Admiral|Captain|Major|Colonel|Lieutenant)\b',
        # Add academic degrees and suffixes
        r'\b(Ph\.D\.?|PhD|M\.D\.?|MD|J\.D\.?|JD|M\.B\.A\.?|MBA|B\.A\.?|BA|M\.S\.?|MS|B\.S\.?|BS)\b',
        r'\b(Sr\.?|Jr\.?|III|IV|V)\b'  # Senior, Junior, Roman numerals
    ]
    
    for title_pattern in titles:
        name = re.sub(title_pattern, '', name, flags=re.IGNORECASE)
    
    # Clean up extra whitespace and punctuation
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r'^\W+', '', name)  # Remove leading punctuation
    name = re.sub(r'\W+$', '', name)  # Remove trailing punctuation
    
    if not name:
        return result
    
    # Split into parts
    parts = name.split()
    
    if len(parts) == 0:
        return result
    
    # Extract family name (last part)
    family_name = parts[-1]
    result.family_norm = normalize_text(family_name)
    
    # Extract given name (first part)
    given_name = parts[0]
    result.given_norm = normalize_text(given_name)
    
    # Extract given initial
    if given_name:
        result.given_initial = normalize_text(given_name[0])
    
    # Extract given prefix (2-3 characters)
    if len(given_name) >= 2:
        prefix_length = min(3, len(given_name))
        result.given_prefix3 = normalize_text(given_name[:prefix_length])
    elif len(given_name) == 1:
        result.given_prefix3 = normalize_text(given_name)
    
    # Extract middle initials (middle parts)
    middle_parts = parts[1:-1] if len(parts) > 2 else []
    middle_initials = []
    
    for part in middle_parts:
        # Remove dots and take first character
        clean_part = part.replace('.', '').strip()
        if clean_part:
            middle_initials.append(normalize_text(clean_part[0]))
    
    if middle_initials:
        result.middle_initials = ''.join(middle_initials)
    
    # Create full normalized name without titles
    name_parts = []
    if result.given_norm:
        name_parts.append(result.given_norm)
    if result.middle_initials:
        name_parts.append(result.middle_initials)
    if result.family_norm:
        name_parts.append(result.family_norm)
    
    if name_parts:
        result.full_norm_no_honor = ' '.join(name_parts)
    
    return result


def normalize_text(text: str) -> str:
    """
    Normalize text using NFKD decomposition, remove diacritics, and convert to lowercase.
    
    Args:
        text: Input text to normalize
        
    Returns:
        Normalized text
    """
    if not text:
        return ""
    
    # NFKD normalization
    normalized = unicodedata.normalize('NFKD', text)
    
    # Remove diacritics (combining characters)
    without_diacritics = ''.join(
        char for char in normalized 
        if not unicodedata.combining(char)
    )
    
    # Convert to lowercase and remove extra whitespace
    result = without_diacritics.lower().strip()
    
    return result


def test_normalize_name():
    """Test function to verify name normalization works correctly."""
    
    test_cases = [
        "Mr. August Specht Ph.D.",
        "Mr. Timothy D. Cook",
        "Dr. Jane Smith",
        "Prof. María José García-López",
        "CEO John A. B. Doe",
        "Timothy Cook",
        "T. Cook",
        "Cook",
        "Jean-Pierre Dupont",
        "José María Fernández",
        ""
    ]
    
    print("Testing name normalization:")
    print("=" * 50)
    
    for test_name in test_cases:
        result = normalize_name(test_name)
        print(f"Input: '{test_name}'")
        print(f"Output: {result}")
        print("-" * 30)


def populate_entities_from_infos(limit: int = None) -> Dict[str, Any]:
    """
    Extract entities from infos table and populate entities/aliases/affiliations.
    Returns stats dict with counts.
    """
    stats = {
        'orgs_created': 0,
        'persons_created': 0,
        'aliases_created': 0,
        'affiliations_created': 0,
        'errors': []
    }
    
    try:
        # Initialize database connection
        db_path = Path(__file__).parent.parent.parent / "data" / "db" / "news.db"
        db = DatabaseConnection(str(db_path))
        
        # Ensure entities tables exist
        if not db.ensure_entities_tables():
            stats['errors'].append("Failed to create entities tables")
            return stats
        
        # Get all infos
        infos_list = db.get_all_infos()
        if limit:
            infos_list = infos_list[:limit]
        print(f"Processing {len(infos_list)} infos records...")
        
        for info in infos_list:
            try:
                symbol = info['symbol']
                print(f"Processing {symbol}...")
                
                # Process organization
                org_entity_id = _process_organization(db, info, stats)
                if org_entity_id is None:
                    continue
                
                # Process officers
                _process_officers(db, info, org_entity_id, stats)
                
            except Exception as e:
                error_msg = f"Error processing {info.get('symbol', 'unknown')}: {str(e)}"
                print(error_msg)
                stats['errors'].append(error_msg)
                continue
        
        print(f"\nProcessing complete!")
        print(f"Organizations created: {stats['orgs_created']}")
        print(f"Persons created: {stats['persons_created']}")
        print(f"Aliases created: {stats['aliases_created']}")
        print(f"Affiliations created: {stats['affiliations_created']}")
        print(f"Errors: {len(stats['errors'])}")
        
        return stats
        
    except Exception as e:
        error_msg = f"Fatal error in populate_entities_from_infos: {str(e)}"
        print(error_msg)
        stats['errors'].append(error_msg)
        return stats


def _process_organization(db: DatabaseConnection, info: Dict[str, Any], stats: Dict[str, Any]) -> Optional[int]:
    """Process organization from info record"""
    try:
        symbol = info['symbol']
        
        # Use long_name, short_name, display_name, or symbol as fallback
        canonical_full = (info.get('long_name') or 
                         info.get('short_name') or 
                         info.get('display_name') or 
                         symbol)
        
        if not canonical_full:
            print(f"Warning: {symbol} has no name fields, skipping")
            return None
        
        # Check if org already exists
        existing = db.get_entity_by_canonical('org', canonical_full=canonical_full)
        if existing:
            raise Exception(f"Organization already exists: {canonical_full}")
        
        # Prepare org fields
        org_fields = {
            'canonical_full': canonical_full,
            'display_name': info.get('display_name') or info.get('short_name') or symbol,
            'long_business_summary': info.get('long_business_summary'),
            'website': info.get('website'),
            'ir_website': info.get('ir_website'),
            'phone': info.get('phone'),
            'address1': info.get('address1'),
            'city': info.get('city'),
            'state': info.get('state'),
            'zip': info.get('zip'),
            'country': info.get('country'),
            'sector': info.get('sector'),
            'industry': info.get('industry'),
            'full_time_employees': info.get('full_time_employees')
        }
        
        # Remove None values
        org_fields = {k: v for k, v in org_fields.items() if v is not None}
        
        # Insert organization
        org_entity_id = db.insert_entity('org', **org_fields)
        stats['orgs_created'] += 1
        
        # Create aliases - only create if the alias text exists and is different from canonical_full
        aliases_to_create = [
            ('symbol', symbol, symbol.lower(), {'is_primary': 1}),
            ('long_name', info.get('long_name'), normalize_text(info.get('long_name', '')), {}),
            ('short_name', info.get('short_name'), normalize_text(info.get('short_name', '')), {}),
            ('display_name', info.get('display_name'), normalize_text(info.get('display_name', '')), {})
        ]
        
        for alias_type, alias_text, normalized, extra_params in aliases_to_create:
            if alias_text and alias_text != canonical_full:  # Avoid duplicate aliases
                db.insert_alias(org_entity_id, alias_text, alias_type, normalized, **extra_params)
                stats['aliases_created'] += 1
        
        return org_entity_id
        
    except Exception as e:
        raise Exception(f"Error processing organization {symbol}: {str(e)}")


def _process_officers(db: DatabaseConnection, info: Dict[str, Any], org_entity_id: int, stats: Dict[str, Any]):
    """Process officers from officers_json"""
    try:
        officers_json = info.get('officers_json')
        if not officers_json:
            return
        
        officers = json.loads(officers_json)
        if not isinstance(officers, list):
            return
        
        for officer in officers:
            try:
                name = officer.get('name', '')
                title = officer.get('title', '')
                
                if not name:
                    continue
                
                # Normalize name
                name_result = normalize_name(name)
                
                # Parse name components from the normalized name (after title removal)
                # Use the normalized name without titles for parsing
                normalized_name = name_result.full_norm_no_honor
                if not normalized_name:
                    continue
                
                name_parts = normalized_name.split()
                if len(name_parts) < 2:
                    continue
                
                given = name_parts[0]
                family = name_parts[-1]
                middle = ' '.join(name_parts[1:-1]) if len(name_parts) > 2 else None
                
                # Check if person already exists
                existing = db.get_entity_by_canonical('person', given=given, family=family)
                if existing:
                    person_entity_id = existing['entity_id']
                else:
                    # Create person entity
                    person_fields = {
                        'given': given,
                        'middle': middle,
                        'family': family,
                        'canonical_full': name,
                        'display_name': f"{given} {family}",
                        'given_norm': name_result.given_norm,
                        'family_norm': name_result.family_norm,
                        'given_initial': name_result.given_initial,
                        'given_prefix3': name_result.given_prefix3,
                        'middle_initials': name_result.middle_initials,
                        'full_norm_no_honor': name_result.full_norm_no_honor
                    }
                    
                    # Remove None values
                    person_fields = {k: v for k, v in person_fields.items() if v is not None}
                    
                    person_entity_id = db.insert_entity('person', **person_fields)
                    stats['persons_created'] += 1
                
                # Create affiliation
                db.insert_affiliation(person_entity_id, org_entity_id, title)
                stats['affiliations_created'] += 1
                
            except Exception as e:
                print(f"Error processing officer {officer.get('name', 'unknown')}: {str(e)}")
                continue
                
    except Exception as e:
        raise Exception(f"Error processing officers for {info.get('symbol', 'unknown')}: {str(e)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Entity extraction and name normalization')
    parser.add_argument('--test', action='store_true', help='Run name normalization tests')
    parser.add_argument('--extract', action='store_true', help='Extract entities from infos')
    
    args = parser.parse_args()
    
    if args.test:
        test_normalize_name()
    elif args.extract:
        populate_entities_from_infos()
    else:
        print("Use --test to run normalization tests or --extract to populate entities")
