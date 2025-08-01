"""경기도를 스크랩.
"""
from bs4 import BeautifulSoup
from scrap.utils.requests import get_selenium, By
from scrap.local_councils import *
from scrap.local_councils.basic import (
    find,
    regex_pattern,
    findall,
    extract_party,
    getname,
    getpty_easy,
    get_soup,
    getprofiles,
)

party_keywords = getPartyList()
party_keywords.append("무소속")


def scrap_76(url, cid, args: ArgsType) -> ScrapResult:
    """경기도 성남시"""
    assert args is not None
    assert args.pf_elt is not None
    assert args.pf_cls is not None
    assert args.name_elt is not None
    assert args.name_cls is not None
    assert args.pty_elt is not None
    assert args.pty_cls is not None

    councilors: list[Councilor] = []

    browser = get_selenium(url)

    councilor_infos = browser.find_elements(
        By.CSS_SELECTOR, args.pf_elt + "[class*='" + args.pf_cls + "']"
    )

    for info in councilor_infos:
        name_tag = info.find_element(
            By.CSS_SELECTOR, args.name_elt + "[class='" + args.name_cls + "']"
        )
        name = name_tag.text.strip() if name_tag else "이름 정보 없음"
        party_tag = info.find_elements(By.TAG_NAME, args.pty_elt)
        party = ""
        for tag in party_tag:
            party = tag.text.strip()
            if party in party_keywords:
                break
        if party not in party_keywords:
            party = "정당 정보 없음"

        councilors.append(Councilor(name, party))

    return ret_local_councilors(cid, councilors)


def scrap_78(url, cid, args: ArgsType) -> ScrapResult:
    """경기도 안양시"""
    browser = get_selenium(url)
    html = browser.page_source
    soup = BeautifulSoup(html, "html.parser")
    councilors: list[Councilor] = []

    profiles = getprofiles(
        soup, args.pf_elt, args.pf_cls, args.pf_memlistelt, args.pf_memlistcls
    )
    # print(cid, "번째 의회에는,", len(profiles), "명의 의원이 있습니다.")  # 디버깅용.

    for profile in profiles:
        name = party = ""
        try:
            name = getname(
                profile,
                args.name_elt,
                args.name_cls,
                args.name_wrapelt,
                args.name_wrapcls,
            )
        except Exception as e:
            raise RuntimeError("[basic.py] 의원 이름을 가져오는데 실패했습니다. 이유 : " + str(e))
        try:
            party = extract_party(profile.text)
        except Exception as e:
            raise RuntimeError("[basic.py] 의원 정당을 가져오는데 실패했습니다. 이유: " + str(e))
        councilors.append(Councilor(name=name, jdName=party))

    return ret_local_councilors(cid, councilors)


def get_profiles_88_103(soup, element, class_, memberlistelement, memberlistclass_):
    if memberlistelement is not None:
        try:
            soup = soup.find_all(memberlistelement, id=memberlistclass_)[0]
        except Exception:
            raise RuntimeError("[basic.py] 의원 목록 사이트에서 의원 프로필을 가져오는데 실패했습니다.")
    return soup.find_all(element, class_)


def get_party_88(profile, element, class_, wrapper_element, wrapper_class_, url):
    if wrapper_element is not None:
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        # 프로필보기 링크 가져오기
        profile_link = find(profile, wrapper_element, class_=wrapper_class_).find("a")
        profile_url = base_url + profile_link["href"]
        profile = get_soup(profile_url, verify=False, encoding="euc-kr")
    party_pulp_list = list(
        filter(
            lambda x: regex_pattern.search(str(x)), findall(profile, element, class_)
        )
    )
    if party_pulp_list == []:
        raise RuntimeError("[basic.py] 정당정보 regex 실패")
    party_pulp = party_pulp_list[0]
    party_string = party_pulp.get_text(strip=True).split(" ")[-1]
    while True:
        if (party := extract_party(party_string)) is not None:
            return party
        if (party_pulp := party_pulp.find_next("span")) is not None:
            party_string = party_pulp.text.strip().split(" ")[-1]
        else:
            return "[basic.py] 정당 정보 파싱 불가"


def scrap_88(url, cid, args: ScrapBasicArgument) -> ScrapResult:
    encoding = "euc-kr"
    soup = get_soup(url, verify=False, encoding=encoding)
    councilors: list[Councilor] = []
    profiles = get_profiles_88_103(
        soup, args.pf_elt, args.pf_cls, args.pf_memlistelt, args.pf_memlistcls
    )
    # print(cid, "번째 의회에는,", len(profiles), "명의 의원이 있습니다.")  # 디버깅용.

    for profile in profiles:
        name = getname(
            profile, args.name_elt, args.name_cls, args.name_wrapelt, args.name_wrapcls
        )
        party = ""
        try:
            party = get_party_88(
                profile,
                args.pty_elt,
                args.pty_cls,
                args.pty_wrapelt,
                args.pty_wrapcls,
                url,
            )
        except Exception:
            party = getpty_easy(
                profile, args.pty_wrapelt, args.pty_wrapcls, args.pty_wraptxt, url
            )

        councilors.append(Councilor(name=name, jdName=party))

    return ret_local_councilors(cid, councilors)


def scrap_97(url, cid, args=None) -> ScrapResult:
    """경기도 이천시"""
    browser = get_selenium(url)
    html = browser.page_source
    soup = BeautifulSoup(html, "html.parser")
    councilors: list[Councilor] = []

    profiles = soup.find("ul", "memberList").find_all("li")
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    for profile in profiles:
        name = profile.text.split(" ")[0]
        browser = get_selenium(base_url + profile.find("a")["href"])
        party = extract_party(BeautifulSoup(browser.page_source, "html.parser").text)
        councilors.append(Councilor(name=name, jdName=party))

    return ret_local_councilors(cid, councilors)


def get_party_103(profile, element, class_, wrapper_element, wrapper_class_, url):
    if wrapper_element is not None:
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        # 프로필보기 링크 가져오기
        profile_link = profile.find(wrapper_element, class_=wrapper_class_)
        profile_url = base_url + "/member/" + profile_link["href"]
        profile = get_soup(profile_url, verify=False)
    party_pulp_list = list(
        filter(
            lambda x: regex_pattern.search(str(x)), findall(profile, element, class_)
        )
    )
    if party_pulp_list == []:
        raise RuntimeError("[basic.py] 정당정보 regex 실패")
    party_pulp = party_pulp_list[0]
    party_string = party_pulp.get_text(strip=True).split(" ")[-1]
    while True:
        if (party := extract_party(party_string)) is not None:
            return party
        if (party_pulp := party_pulp.find_next("span")) is not None:
            party_string = party_pulp.text.strip().split(" ")[-1]
        else:
            return "[basic.py] 정당 정보 파싱 불가"


def scrap_103(url, cid, args: ScrapBasicArgument) -> ScrapResult:
    cid = 103
    soup = get_soup(url, verify=False)
    councilors: list[Councilor] = []
    profiles = get_profiles_88_103(
        soup, args.pf_elt, args.pf_cls, args.pf_memlistelt, args.pf_memlistcls
    )
    # print(cid, "번째 의회에는,", len(profiles), "명의 의원이 있습니다.")  # 디버깅용.

    for profile in profiles:
        name = getname(
            profile, args.name_elt, args.name_cls, args.name_wrapelt, args.name_wrapcls
        )
        party = get_party_103(
            profile, args.pty_elt, args.pty_cls, args.pty_wrapelt, args.pty_wrapcls, url
        )

        councilors.append(Councilor(name=name, jdName=party))

    return ret_local_councilors(cid, councilors)
