"""Temp script to search CSV descriptions for correct matching - round 2."""
import csv, re

rows = []
for f in ['data/turnover_and_website_202604161632.csv', 'data/turnover_and_website_202604161634.csv']:
    with open(f) as fh:
        for r in csv.DictReader(fh):
            rows.append((r['id'], r['description']))

terms = {
    'scope2': r'scope.?2.?(emission|total)',
    'total-energy-within': r'total.*energy.*within|energy.*consumption.*total.*within|energy.*inside',
    'total-fuel-within': r'fuel.*consumption.*total|total.*fuel.*within',
    'total-elec-within': r'total.*electricity|electricity.*total',
    'water-consumed-total': r'water.*consumed|total.*water.*consumption',
    'water-recyc-reuse': r'recycl|reuse|reclaim',
    'hazardous-total': r'total.*hazardous(?!.*non)',
    'non-haz-total': r'total.*non.*hazardous',
    'board-total-all': r'total.*number.*board|board.*total|board.*of.*director.*total',
    'independent': r'independent.*director|non.?executive',
    'women-board-pct': r'female.*board.*percent|board.*female.*percent|percentage.*female.*board',
    'women-mgmt-pct': r'female.*(key|manage|senior).*percent|percentage.*female.*(manage|key)',
    'lost-time-lti': r'lost.?time|ltifr|lti.*rate',
    'trir-record': r'recordable.*work.*injur',
    'lost-work-day': r'(lost|absence).*(work)?.*day|day.*lost|number.*days.*lost',
    'avg-train-hr': r'average.*training|training.*per.*employee|per.*employee.*training',
    'train-invest': r'training.*(cost|invest|spend|amount)',
    'train-hour-male': r'training.*hour.*male|male.*training.*hour',
    'train-hour-female': r'training.*hour.*female|female.*training.*hour',
    'parental-male-took': r'male.*took.*parental|male.*employee.*parental.*leave$',
    'parental-female-took': r'female.*took.*parental|female.*employee.*parental.*leave$',
    'parental-return-male': r'male.*return.*parental',
    'parental-return-female': r'female.*return.*parental',
    'parental-rate': r'parental.*leave.*(rate|percent|retention)',
    'perf-review': r'performance.*review|career.*development.*review',
    'gender-pay': r'gender.*pay|pay.*gap.*gender|ratio.*male.*female.*pay',
    'whistle': r'whistle|ethics.*hotline|raise.*concern.*business.*conduct',
    'customer-privacy': r'customer.*privacy|substantiated.*complaint',
    'emission-reduce': r'emmission.*reduction|emission.*reduction',
}

for label, pat in sorted(terms.items()):
    matches = [(rid, desc) for rid, desc in rows if re.search(pat, desc, re.IGNORECASE)]
    if matches:
        print(f'--- {label} ({len(matches)}) ---')
        for rid, desc in matches[:4]:
            print(f'  {rid} | {desc}')
    else:
        print(f'--- {label}: NO MATCH ---')
