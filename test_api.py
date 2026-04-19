"""Test PDDikti API endpoints to understand data structure."""
import requests
import json
import sys

HEADERS = {
    'Origin': 'https://pddikti.kemdiktisaintek.go.id',
    'Referer': 'https://pddikti.kemdiktisaintek.go.id/',
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json',
}
BASE = 'https://api-pddikti.kemdiktisaintek.go.id'


def fetch(endpoint):
    try:
        r = requests.get(f'{BASE}/{endpoint}', headers=HEADERS, timeout=30)
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception as e:
        print(f"  Error: {e}")
        return None


def test_prodi_discovery():
    """Test finding all prodi for a rumpun."""
    print("=" * 60)
    print("TEST 1: Prodi Discovery for ZAKAT DAN WAKAF")
    print("=" * 60)

    data = fetch("pencarian/prodi/ZAKAT DAN WAKAF")
    if not data:
        print("No data!")
        return []

    print(f"Total prodi found: {len(data)}")
    for p in data:
        nama = p.get('nama', '')
        jenjang = p.get('jenjang', '')
        pt = p.get('pt', '')
        print(f"  {nama} - {jenjang} - {pt}")

    return data


def test_dosen_for_prodi(prodi_id, label, semesters=None):
    """Test fetching dosen for a specific prodi."""
    print(f"\n--- Dosen for: {label} ---")

    # Try without semester
    for ep in ['homebase', 'penugasan']:
        data = fetch(f"dosen/{ep}/{prodi_id}")
        count = len(data) if isinstance(data, list) else 0
        print(f"  {ep} (no semester): {count} dosen")
        if count > 0:
            for d in data[:3]:
                print(f"    - {d.get('nama_dosen', '?')} / NIDN: {d.get('nidn', '?')}")

    # Try with semesters
    if semesters:
        total_found = set()
        for sem in semesters:
            for ep in ['homebase', 'penugasan']:
                data = fetch(f"dosen/{ep}/{prodi_id}?semester={sem}")
                if isinstance(data, list) and len(data) > 0:
                    for d in data:
                        nidn = d.get('nidn', '')
                        nama = d.get('nama_dosen', '')
                        key = nidn if nidn else nama
                        if key and key not in total_found:
                            total_found.add(key)
                            print(f"    [{ep} sem={sem}] {nama} / NIDN: {nidn}")
        print(f"  Total unique dosen across semesters: {len(total_found)}")


def test_prodi_dosen_endpoint(prodi_id, label):
    """Try the prodi/dosen endpoint (list all dosen for a prodi, all time)."""
    print(f"\n--- prodi/dosen endpoint for: {label} ---")
    data = fetch(f"prodi/dosen/{prodi_id}")
    if data and isinstance(data, list):
        print(f"  Found: {len(data)} dosen")
        for d in data[:5]:
            print(f"    - {json.dumps(d)}")
    else:
        print(f"  No data or error: {type(data)}")


def test_search_dosen_directly():
    """Try searching dosen directly by a known name."""
    print("\n" + "=" * 60)
    print("TEST: Search dosen directly")
    print("=" * 60)

    data = fetch("pencarian/dosen/ekonomi syariah")
    if data and isinstance(data, list):
        print(f"  Found: {len(data)} dosen")
        for d in data[:5]:
            print(f"    - {d.get('nama', '?')} / NIDN: {d.get('nidn', '?')} / PT: {d.get('nama_pt', '?')}")
    else:
        print(f"  No data")


def test_dosen_profile(dosen_id):
    """Test fetching a specific dosen profile."""
    print(f"\n--- Dosen Profile ---")
    data = fetch(f"dosen/profile/{dosen_id}")
    if data:
        print(json.dumps(data, indent=2))
    else:
        print("  No data")


def test_all_semesters_scan(prodi_id, label):
    """Scan ALL semesters for a prodi to find all dosen ever."""
    print(f"\n--- Full semester scan for: {label} ---")
    all_semesters = [
        "20242", "20241", "20232", "20231",
        "20222", "20221", "20212", "20211", "20202", "20201",
        "20192", "20191", "20182", "20181", "20172", "20171",
        "20162", "20161", "20152", "20151", "20142", "20141",
        "20132", "20131", "20122", "20121", "20112", "20111",
    ]
    all_dosen = {}
    for sem in all_semesters:
        for ep in ['homebase', 'penugasan']:
            data = fetch(f"dosen/{ep}/{prodi_id}?semester={sem}")
            if isinstance(data, list):
                for d in data:
                    nidn = d.get('nidn', '')
                    nama = d.get('nama_dosen', '')
                    key = nidn if nidn else nama
                    if key and key not in all_dosen:
                        all_dosen[key] = {
                            'nama': nama,
                            'nidn': nidn,
                            'semester': sem,
                            'endpoint': ep,
                            'status': d.get('status_aktif', ''),
                        }
    print(f"  Total unique dosen across ALL semesters: {len(all_dosen)}")
    for key, info in all_dosen.items():
        print(f"    - {info['nama']} / NIDN: {info['nidn']} / First found: {info['semester']} via {info['endpoint']} / Status: {info['status']}")


if __name__ == '__main__':
    test = sys.argv[1] if len(sys.argv) > 1 else 'discover'

    if test == 'discover':
        prodi_list = test_prodi_discovery()

    elif test == 'dosen_quick':
        # Test first IAIN PURWOKERTO prodi
        prodi_id = 'wYSupkAACR0fmhLbA9OqbmWwCh5Sru6uk1TKBN8KaRHq_m2EyOKB9dxGDl1vTICk1YMCGA=='
        test_dosen_for_prodi(prodi_id, "IAIN PURWOKERTO - MZW", ['20251', '20242', '20241', '20232', '20231', '20222', '20221', '20212', '20211'])
        test_prodi_dosen_endpoint(prodi_id, "IAIN PURWOKERTO - MZW")

    elif test == 'scan_full':
        # Full scan for STAIN TULUNGAGUNG
        prodi_id = 'XAe1D87cDzfM1DdW9NPJhoTLCHRXAMvSgN1sa7whr-5zH5-SixhYufnaxnQoLPyIpTCQXw=='
        test_all_semesters_scan(prodi_id, "STAIN TULUNGAGUNG - ZW")

    elif test == 'scan_iain':
        # Full scan for IAIN PURWOKERTO
        prodi_id = 'wYSupkAACR0fmhLbA9OqbmWwCh5Sru6uk1TKBN8KaRHq_m2EyOKB9dxGDl1vTICk1YMCGA=='
        test_all_semesters_scan(prodi_id, "IAIN PURWOKERTO - MZW")

    elif test == 'search_dosen':
        test_search_dosen_directly()

    elif test == 'test_all_prodi':
        # Test ALL prodi from ZAKAT DAN WAKAF
        prodi_list = test_prodi_discovery()
        for p in prodi_list[:5]:
            pid = p['id']
            label = f"{p['nama']} - {p['pt']}"
            for ep in ['homebase', 'penugasan']:
                data = fetch(f"dosen/{ep}/{pid}")
                count = len(data) if isinstance(data, list) else 0
                if count > 0:
                    print(f"  {ep} (no sem) for {label}: {count} dosen")
                    for d in data[:2]:
                        print(f"    - {d.get('nama_dosen', '?')} / NIDN: {d.get('nidn', '?')}")
