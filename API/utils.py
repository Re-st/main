import os
import requests
import pandas as pd
from typing import List, Optional

from . import BASE_DIR, SG_TYPECODE, SG_TYPECODE_TYPE
from configurations.secrets import MongoDBSecrets
from db.client import client


def save_to_excel(data: List[dict], sgTypecode: str, is_elected: bool) -> None:
    directory_path = os.path.join(BASE_DIR, "output")
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    excel_file = f"[{'당선' if is_elected else '후보'}][{SG_TYPECODE[sgTypecode]}].xlsx"
    df = pd.DataFrame(data)
    df.to_excel(os.path.join(directory_path, excel_file), index=False)

    print(f"데이터를 성공적으로 '{excel_file}'에 저장하였습니다.")


def get_local_district_id(sd_name: str, wiw_name: str) -> Optional[int]:
    db = client["district"]
    if "시" in wiw_name and "구" in wiw_name:
        wiw_name = wiw_name[: wiw_name.find("시") + 1]  # 포항시북구 -> 포항시

    district_doc = db["local_district"].find_one(
        {"sdName": sd_name, "wiwName": wiw_name}
    )
    return district_doc["cid"] if district_doc else None


def save_to_mongo(data: List[dict], sgTypecode: str) -> None:
    db = client[str(MongoDBSecrets.database_name)]
    main_collection = db[str(SG_TYPECODE_TYPE[sgTypecode])]

    # TODO: Support other types of councils
    if sgTypecode == "6":
        for entry in data:
            district_id = get_local_district_id(entry["sdName"], entry["wiwName"])
            entry.pop("sdName")
            entry.pop("wiwName")

            if district_id:
                main_collection.update_one(
                    {
                        "council_id": district_id,
                        "council_type": SG_TYPECODE_TYPE[sgTypecode],
                    },
                    {"$push": {"councilors": entry}},
                    upsert=True,
                )
            else:
                print(
                    f"Warning: '{entry['sdName']} {entry['wiwName']}'에 해당하는 지역 ID가 존재하지 않습니다."
                )
    else:
        raise NotImplementedError("현재 구시군의회의원(6)만 구현되어 있습니다.")

    print(f"데이터를 성공적으로 MongoDB '{main_collection.name}' 컬렉션에 저장하였습니다.")
