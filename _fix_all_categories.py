"""
DEFINITIVE category fix — sweeps ALL JSON files in docs/.
Fixes:
  1) Critical errors: pubgm_data.json (Messi, Balenciaga, etc. still Vehicle)
  2) LIVERPOOL everywhere → Other
  3) Weibo mass misclassification (hundreds of non-vehicle brands as Vehicle)
  4) YouTube regional files: any remaining non-vehicle brands
"""
import json, os, sys, io
from collections import defaultdict
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DOCS = 'docs'

# ===== DEFINITIVE VEHICLE BRANDS (real automobile/motorcycle companies) =====
REAL_VEHICLES = {
    'BUGATTI', 'KOENIGSEGG', 'LAMBORGHINI', 'MCLAREN', 'PAGANI', 'DODGE',
    'MASERATI', 'ASTON MARTIN', 'PORSCHE', 'BMW', 'MERCEDES', 'MERCEDES-BENZ',
    'FERRARI', 'SHELBY', 'DUCATI', 'FORD', 'TESLA', 'NISSAN', 'TOYOTA', 'HONDA',
    'YAMAHA', 'KTM', 'HARLEY', 'JEEP', 'MINI COOPER', 'ROLLS ROYCE', 'BENTLEY',
    'RANGE ROVER', 'LAND ROVER', 'AUDI', 'VOLKSWAGEN', 'VOLKSWAGEN BEETLE SET',
    'DACIA', 'LOTUS', 'LOTUS CARS', 'POLARIS', 'SCUDERIA FERRARI',
    'SCUDERIA FERRARI HP', 'SHELBY AMERICAN', 'WULING MOTORS',
    'HERO', 'HERO XTREME', 'ROYAL ENFIELD', 'MAHINDRA', 'INDIAN MOTORCYCLE',
    'PULSAR', 'SSC', 'BYD', 'BEIJING HYUNDAI', 'YANGWANG AUTO, BYD AUTO',
    'YANGWANG U7', 'NIU TECHNOLOGIES', 'CYBERTRUCK & ROADSTER',
    'GEORGE PATTON VEHICLE', 'MCLAREN F1', 'MCLAREN F1 TEAM',
    'FIA FORMULA E CHAMPIONSHIP', 'F1', 'SHENZHEN BUS GROUP',
    'CHONGQING RAIL TRANSIT', 'AIRBUS', 'MOANA YACHT',
    'SHANGHAI INTERNATIONAL CIRCUIT',
}

# ===== EXPLICIT CORRECTIONS (partner_name_upper → correct_category) =====
EXPLICIT_FIX = {
    # Sports
    'LIONEL MESSI': 'Other',
    'LIVERPOOL': 'Other',
    'LIVERPOOL FC': 'Other',
    'AFA': 'Other',
    'NBA STYLE': 'Other',
    'MIGU SPORTS': 'Other',
    'TENCENT SPORTS': 'Other',
    'EDC CHINA': 'Other',
    'EDC ELECTRIC DAISY CARNIVAL': 'Other',
    'PEAK': 'Other',
    '361°': 'Other',
    'YIKUN DISCS': 'Other',

    # Fashion
    'BALENCIAGA': 'Fashion',
    'DANIEL WELLINGTON': 'Fashion',
    'DW': 'Fashion',
    'PEACEBIRD': 'Fashion',
    'PEACEBIRD MEN': 'Fashion',
    'PUYUAN FASHION WEEK': 'Fashion',
    'BEYOND THE BOUNDARY': 'Fashion',
    'BEYOND THE BOUNDARY FASHION': 'Fashion',
    'CLASSICAL PUPPETS': 'Fashion',
    'ROSEONLY': 'Fashion',
    'TREND BRAND': 'Fashion',
    'ZHIYIN CULTURAL CREATION WOODERFUL LIFE': 'Fashion',
    'LIFESTYLE BRAND': 'Fashion',
    'PANZI WOMEN\'S WORKSHOP': 'Fashion',
    'ESQUIRE': 'Fashion',

    # Character/Toy
    'BE@RBRICK': 'Character',
    'LITTLE PARROT BEBE': 'Character',
    'LITTLE LUBAN': 'Character',
    'TOPTOY': 'Character',
    'JOYTOY DARK SOURCE': 'Character',
    'LINE FRIENDS': 'Character',
    'WARHORSE': 'Character',
    'FLASH FISH': 'Character',
    'MAGPIE': 'Character',
    'TWINFLOWER': 'Character',
    'LAMBO CAR SKINS': 'Character',

    # Tech/Electronics
    'SAMSUNG': 'Other',
    'QUALCOMM': 'Other',
    'VIVO': 'Other',
    'ONEPLUS': 'Other',
    'XPPEN': 'Other',
    'XP-PEN': 'Other',
    'XPERIA 1 IV': 'Other',
    'APPLE': 'Other',
    'HONOR': 'Other',
    'HUAWEI': 'Other',
    'SONY': 'Other',
    'BASEUS': 'Other',
    '1MORE': 'Other',
    'AD HELMET': 'Other',
    'HECATE EDIFIER GAMING': 'Other',
    'LOGITECH': 'Other',
    'LOGITECH G': 'Other',
    'MAO KING SPEAKERS': 'Other',
    'MAO KING WILD MINI': 'Other',
    'SKYWORK SHAVER': 'Other',
    'MI HOME': 'Other',
    'CHINA MOBILE': 'Other',
    'MEITU XIUXIU': 'Other',

    # F&B (Food & Beverage)
    'KFC': 'Other',
    'BURGER KING': 'Other',
    'GRUBHUB': 'Other',
    'MCDONALD\'S, ONEPLUS, STRIDE, 361 DEGREES, PRINGLES, CHOW TAI SENG, LEKE FITNESS, IM-BODY, SEASONS FRAGRANCE, CLEERAUDIO': 'Other',
    'COCA-COLA': 'Other',
    'WANGLAOJI': 'Other',
    'GENKI FOREST': 'Other',
    'MENGNIU SOUR MILK': 'Other',
    'MASTER KONG': 'Other',
    'MASTER KONG SPICY BEEF NOODLES': 'Other',
    'HAAGEN-DAZS': 'Other',
    'SHAKE SHACK': 'Other',
    'KRISPY KREME': 'Other',
    'MOUNTAIN DEW': 'Other',
    'RED BULL': 'Other',
    'TECATE': 'Other',
    'WINGSTOP': 'Other',
    'SMALL CAN TEA': 'Other',
    '1点点奶茶': 'Other',
    'NIN JIOM PEI PA KOA': 'Other',
    'LIULIU MEI': 'Other',
    'BING XIAOCHA': 'Other',
    '999 COLD REMEDY': 'Other',
    'DAOXIANGCUN': 'Other',
    'WANG XIAOLU': 'Other',
    'GUI MAN LONG': 'Other',
    'BANTIANYAO GRILLED FISH': 'Other',
    'XIANHEZHUANG': 'Other',
    'XIANHEZHUANG BRAISED FLAVOR HOTPOT': 'Other',
    'MEET XIAOMIAN': 'Other',
    'HANKOU NO.2 FACTORY': 'Other',
    'ICE FACTORY': 'Other',
    'SUPER WENHEYOU': 'Other',
    'DICOS': 'Other',
    'DICOS, CTRIP, QEELIN JEWELRY, MEITUAN HOTELS, YUNNAN BAIYAO': 'Other',
    'LAWSON': 'Other',
    'LAWSON CONVENIENCE STORE': 'Other',
    'HEMA': 'Other',
    'NING JI': 'Other',
    'DINGDONG MAICAI': 'Other',
    'SAISHA': 'Other',
    'YUNHAIYAO, YUNI ZAIYIQI, THREE SQUIRRELS, ZHOU HEI YA': 'Other',
    'TEA RESCUE PLANET': 'Other',

    # Tech Platforms/Apps
    'AMAZON PRIME': 'Other',
    'TENCENT ROG': 'Other',
    'TENCENT MEETING': 'Other',
    'TENCENT WEISHI': 'Other',
    'TENCENT MAPS': 'Other',
    'TENCENT DOCS': 'Other',
    'TENCENT APP STORE': 'Other',
    'TENCENT BONBON GAME': 'Other',
    'TENCENT MEDPEDIA': 'Other',
    'TENCENT SOGOU INPUT METHOD': 'Other',
    'TENCENT MOBILITY SERVICES': 'Other',
    'TENCENT QQ SUPER MEMBER (腾讯QQ超级会员)': 'Other',
    'TENCENT QQ SUPER MEMBER, TENCENT ANIMATION, PENGUIN ESPORTS, TENCENT VIDEO, WEIBO GAME REPORT (腾讯QQ超级会员, 腾讯动漫, 企鹅电竞, 腾讯视频, 微博游戏播报)': 'Other',
    'DOUYIN': 'Other',
    'DOUYIN GAMES': 'Other',
    'DOUYU LIVE STREAMING PLATFORM': 'Other',
    'DOUYU, HUYA, KUAISHOU': 'Other',
    'KUAISHOU GAMES': 'Other',
    'QQ': 'Other',
    'WEIBO': 'Other',
    'WEISHI': 'Other',
    'WESING': 'Other',
    'WECHAT': 'Other',
    'WEGAME': 'Other',
    'BAIDU TIEBA': 'Other',
    'BAIDU INPUT METHOD': 'Other',
    'IFLYTEK INPUT METHOD': 'Other',
    'XIAOHONGSHU': 'Other',
    'XIMALAYA': 'Other',
    'IQIYI': 'Other',
    'KUGOU MUSIC': 'Other',
    'BILIBILI MEMBERSHIP PURCHASE': 'Other',
    'KEEP': 'Other',
    'APPSTORE': 'Other',
    'TAPTAP': 'Other',
    'ZHIHU': 'Other',
    'MEITUAN WAIMAI': 'Other',
    'ELE.ME': 'Other',
    'AMAP': 'Other',
    'TT VOICE': 'Other',
    'CTRIP': 'Other',
    'TONGCHENG TICKETS': 'Other',
    'YOUKU GAMES': 'Other',
    'GAME ZHIJI': 'Other',
    'JD HEALTH': 'Other',
    'JD LOGISTICS': 'Other',
    'LOGITECH, NVIDIA, OMEN, HYPERX, LENOVO LEGION, REDMAGIC, WD_BLACK, ANDASEAT, JD GAMING': 'Other',

    # Media/Entertainment
    'MANGO TV, 100% PRODUCTION': 'Other',
    'CHINA LITERATURE': 'Other',
    'KUAIKAN MANHUA': 'Other',
    'WANDA FILM': 'Other',
    'WANDA CINEMA': 'Other',
    'STAR CINEMA': 'Other',
    'MASTER FILM': 'Other',

    # Locations/Tourism
    'CANTON TOWER': 'Other',
    'BEIJING HAPPY VALLEY': 'Other',
    'BEIJING UNIVERSAL RESORT': 'Other',
    'SHANGHAI HAPPY VALLEY': 'Other',
    'HAPPY VALLEY GROUP': 'Other',
    'VANKE SONGHUA LAKE SKI RESORT': 'Other',
    'SONGHUA LAKE RESORT': 'Other',
    'FIVE CATS ENTERTAINMENT MALL': 'Other',
    'INS NEW PARADISE': 'Other',
    'CHAOTIANMEN WHARF': 'Other',
    'PLANET NO. 6': 'Other',

    # Finance/Services
    'CHINA CITIC BANK': 'Other',
    'CHINA CITIC BANK CREDIT CARD': 'Other',
    'CHINA POST': 'Other',
    'ZHOU LIUFU': 'Other',
    'ZIPPO CHINA': 'Other',

    # Misc brands
    'THE BEAST': 'Other',
    'AMERICAN TOURISTER': 'Other',
    'P.D.P': 'Other',
    'PTOPIA': 'Other',
    'BEAT DROP': 'Other',
    'O-TWO': 'Other',
    'KALKI': 'Other',
    'FAMOUS FOOD DELIVERY APP': 'Other',
    'METROPOLITAN': 'Other',
    '20,000+ BRAND STORES': 'Other',
    'INDUSTRY QUALITY PARTNER': 'Other',
    'COMMERCIAL BRAND, CULTURAL TOURISM IP': 'Other',
    'SUPER FAMILY': 'Other',
    'SIX STAR SIX': 'Other',
    'SIXSTAR': 'Other',
    'GREAT FORTUNE': 'Other',
    'BANCIZUAN': 'Other',
    'LUOYANG POST': 'Other',
    'ZHI SHUO BALA': 'Other',
    'RUIDIAO': 'Other',

    # Film
    'PEAKY BLINDERS': 'Film',
    'VENOM': 'Film',

    # Animation
    'DEMON SLAYER': 'Animation',
    'KIMETSU NO YAIBA': 'Animation',

    # Artist
    'JVKE': 'Artist',
    'AESPA': 'Artist',
}

total_fixes = 0
fixes_by_file = defaultdict(list)

json_files = [f for f in os.listdir(DOCS) if f.endswith('.json') and not f.startswith('.')]
print(f"Scanning {len(json_files)} JSON files...")

for jf in sorted(json_files):
    path = os.path.join(DOCS, jf)
    with open(path, 'r', encoding='utf-8') as f:
        raw = f.read()
    data = json.loads(raw)
    
    if isinstance(data, dict):
        items = list(data.values()) if all(isinstance(v, dict) for v in data.values()) else [data]
        is_dict = True
    else:
        items = data
        is_dict = False
    
    changed = False
    for p in items:
        if not isinstance(p, dict):
            continue
        name = (p.get('partner_name') or p.get('name') or p.get('channel_name') or '').strip()
        name_upper = name.upper()
        cat_key = 'category' if 'category' in p else 'partner_category'
        old_cat = (p.get(cat_key) or '').strip()
        
        new_cat = None
        
        if name_upper in EXPLICIT_FIX:
            if old_cat != EXPLICIT_FIX[name_upper]:
                new_cat = EXPLICIT_FIX[name_upper]
        elif old_cat == 'Vehicle' and name_upper not in REAL_VEHICLES:
            new_cat = 'Other'
        
        if new_cat:
            p[cat_key] = new_cat
            fixes_by_file[jf].append(f"{name}: {old_cat} → {new_cat}")
            total_fixes += 1
            changed = True
    
    if changed:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data if not is_dict else data, f, ensure_ascii=False, indent=2)

print(f"\nTotal fixes applied: {total_fixes}")
for jf, fixes in sorted(fixes_by_file.items()):
    print(f"\n  {jf}: {len(fixes)} fixes")
    for fix in fixes[:20]:
        print(f"    {fix}")
    if len(fixes) > 20:
        print(f"    ... and {len(fixes) - 20} more")

if total_fixes == 0:
    print("\nNo fixes needed — all categories are correct!")
else:
    print(f"\n✅ {total_fixes} total corrections applied across {len(fixes_by_file)} files")
