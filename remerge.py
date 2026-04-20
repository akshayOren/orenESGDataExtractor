"""Re-merge vision_unmapped using updated VISION_METRIC_MAP. No API calls."""
import json, sys
sys.path.insert(0, 'scripts')
from esg_extractor import ESGExtractor, LLMClient

path = 'extracted_output_v8/Abu_Dhabi_Commercial_Bank_2024-04-26/esg_data.json'
d = json.loads(open(path, encoding='utf-8').read())

unmapped = d.pop('vision_unmapped', [])
if not unmapped:
    print('Nothing to re-merge.')
    sys.exit(0)

llm = LLMClient(model='test', simulate=True)
ext = ESGExtractor(llm)

reporting_year = d.get('report_metadata', {}).get('reporting_year')
print(f'Reporting year: {reporting_year}')
print(f'Points to re-merge: {len(unmapped)}')

merged, still_unmapped = ext._merge_vision_data(d, unmapped, reporting_year)

if still_unmapped:
    d['vision_unmapped'] = still_unmapped

# Update metadata
meta = d.get('extraction_metadata', {})
meta['vision_merged'] = meta.get('vision_merged', 0) + merged
meta['vision_unmapped'] = len(still_unmapped)

with open(path, 'w', encoding='utf-8') as f:
    json.dump(d, f, indent=2, ensure_ascii=False)

print(f'Merged: {merged} new fields')
print(f'Still unmapped: {len(still_unmapped)}')
print(f'Saved to {path}')
