#!/usr/bin/env python

########################################################################################
## masssave_reader.py
## Switchbox
## David Karp
## 2025-06-12
##
## This is a function to access some data MassSave
## https://viewer.dnv.com/macustomerprofile/entity/1444/report/2078
##
## Currently it only supports Residential: Electrification data##
##
########################################################################################


import asyncio
import itertools
import json
from datetime import datetime
from typing import Any

import polars as pl
import requests
from attrs import define, field
from attrs.validators import in_
from playwright.async_api import async_playwright

SHARED_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://app.powerbi.com",
    "referer": "https://app.powerbi.com/",
    "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "x-powerbi-hostenv": "Embed for Customers",
}

# COLUMNS_TO_SOURCES = {
#     "Suppression option": "s",
#     "YEAR": "a",
#     "Is_track_new_meas": "f",
#     "Suppression status": "s1",
#     "Option": "s2",
#     "New_Construction_Rule": "n",
#     "Suppression_visible": "s3",
# }

FILTERS_TO_SELECTORS = {
    "End use": "Dim_End_use",
    "Rate_category": "Dim_Rate_Category",
    "Displaced_fuel": "Dim_Displaced_fuel",
    "Year": "Dim_Year",
}
OPERATORS = ["In", "Less_than", "Greater_than"]


@define()
class MassSaveFilter:
    column: str = field(validator=in_(FILTERS_TO_SELECTORS.keys()))
    values: list[str] = field()
    operator: str = field(default="In", validator=in_(OPERATORS))
    invert: bool = field(default=False)

    @classmethod
    def show_columns(cls) -> list[str]:
        return list(FILTERS_TO_SELECTORS.keys())

    def selector_column(self) -> str:
        return FILTERS_TO_SELECTORS[self.column]

    def to_dict(self, source_ref: str) -> dict:
        match self.operator:
            case "In":
                cond = {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": source_ref}},
                                    "Property": self.column,
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{v}'"}} for v in self.values]],
                    }
                }
            case "Greater_than":
                assert len(self.values) == 1, "Greater than filter must have exactly one value"  # noqa: S101
                cond = {
                    "Comparison": {
                        "ComparisonKind": 2,
                        "Left": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": source_ref}},
                                "Property": self.column,
                            }
                        },
                        "Right": {"Literal": {"Value": self.values[0]}},
                    }
                }
            case "Less_than":
                assert len(self.values) == 1, "Less than filter must have exactly one value"  # noqa: S101
                cond = {
                    "Comparison": {
                        "ComparisonKind": 2,
                        "Left": {"Literal": {"Value": self.values[0]}},
                        "Right": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": source_ref}},
                                "Property": self.column,
                            }
                        },
                    }
                }
            case _:
                raise ValueError(f"Invalid operator: {self.operator}")  # noqa: TRY003
        if self.invert:
            return {"Condition": {"Not": cond}}
        return {"Condition": cond}


@define()
class MassSaveQuery:
    filters: list[MassSaveFilter] = field(default=None)
    endpoint_url: str = field(
        default="https://wabi-north-europe-e-primary-redirect.analysis.windows.net/explore/querydata?synchronous=true"
    )
    payload: dict = field(init=False)

    def __attrs_post_init__(self) -> None:
        self.payload = {"version": "1.0.0", "cancelQueries": [], "modelId": 2256951, "userPreferredLocale": "en-US"}

    def _create_filters(self, filters: list[dict]) -> list[dict]:
        if filters is None:
            return ""
        return filters

    def _create_query(self) -> dict[str, Any]:
        query = {
            "Version": 2,
            "From": [
                {"Name": "f", "Entity": "Fact", "Type": 0},
                {"Name": "s", "Entity": "Suppression table", "Type": 0},
                {"Name": "a", "Entity": "ACS", "Type": 0},
                {"Name": "s1", "Entity": "Suppression global switch", "Type": 0},
                {"Name": "s2", "Entity": "Suppression style", "Type": 0},
                {"Name": "n", "Entity": "New Construction Rule", "Type": 0},
                {"Name": "s3", "Entity": "Suppression display control", "Type": 0},
                {"Name": "c", "Entity": "Dim_City", "Type": 0},
                {"Name": "h", "Entity": "Dim_Heat_pump", "Type": 0},
                {"Name": "s4", "Entity": "Dim_Sector", "Type": 0},
            ]
            + [{"Name": f"d{i}", "Entity": f.selector_column(), "Type": 0} for i, f in enumerate(self.filters)],
            "Select": [
                {
                    "Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "City"},
                    "Name": "Dim_City.City",
                    "NativeReferenceName": "Municipality",
                },
                {
                    "Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Participants"},
                    "Name": "Fact.Participants",
                    "NativeReferenceName": "Installed heat pumps (accounts)1",
                },
                {
                    "Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Participanting locations"},
                    "Name": "Fact.Participanting locations",
                    "NativeReferenceName": "Installed heat pumps (locations)",
                },
            ],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Source": "s"}},
                                        "Property": "Suppression option",
                                    }
                                }
                            ],
                            "Values": [[{"Literal": {"Value": "'City'"}}]],
                        }
                    }
                },
                # {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "End use"}}], "Values": [[{"Literal": {"Value": "'HVAC'"}}]]}}},
                # {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d1"}}, "Property": "Year"}}], "Values": [[{"Literal": {"Value": "2019L"}}]]}}},
                {
                    "Condition": {
                        "Not": {
                            "Expression": {
                                "In": {
                                    "Expressions": [
                                        {
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "h"}},
                                                "Property": "Heat_pump",
                                            }
                                        }
                                    ],
                                    "Values": [[{"Literal": {"Value": "'Energy efficiency'"}}]],
                                }
                            }
                        }
                    }
                },
                # {"Condition": {"Comparison": {"ComparisonKind": 2, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "a"}}, "Property": "YEAR"}}, "Right": {"Literal": {"Value": "2019L"}}}}},
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {"Column": {"Expression": {"SourceRef": {"Source": "s4"}}, "Property": "Sector"}}
                            ],
                            "Values": [[{"Literal": {"Value": "'Residential'"}}]],
                        }
                    }
                },
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Source": "f"}},
                                        "Property": "Is_track_new_meas",
                                    }
                                }
                            ],
                            "Values": [[{"Literal": {"Value": "1L"}}]],
                        }
                    }
                },
                {
                    "Condition": {
                        "Not": {
                            "Expression": {
                                "In": {
                                    "Expressions": [
                                        {
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "h"}},
                                                "Property": "Heat_pump",
                                            }
                                        }
                                    ],
                                    "Values": [[{"Literal": {"Value": "'No heat pump'"}}]],
                                }
                            }
                        }
                    }
                },
                ## IMPORTANT: Suppression status is OFF
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Source": "s1"}},
                                        "Property": "Suppression status",
                                    }
                                }
                            ],
                            "Values": [[{"Literal": {"Value": "'Suppression OFF'"}}]],
                        }
                    }
                },
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {"Column": {"Expression": {"SourceRef": {"Source": "s2"}}, "Property": "Option"}}
                            ],
                            "Values": [[{"Literal": {"Value": "'*'"}}]],
                        }
                    }
                },
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Source": "n"}},
                                        "Property": "New_Construction_Rule",
                                    }
                                }
                            ],
                            "Values": [[{"Literal": {"Value": "true"}}]],
                        }
                    }
                },
                # This could maybe flip to False? It doesn't appear to affect the results
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Source": "s3"}},
                                        "Property": "Suppression_visible",
                                    }
                                }
                            ],
                            "Values": [[{"Literal": {"Value": "false"}}]],
                        }
                    }
                },
            ]
            + [f.to_dict(f"d{i}") for i, f in enumerate(self.filters)]
            if self.filters is not None
            else [],
            "OrderBy": [
                {
                    "Direction": 1,
                    "Expression": {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": "City"}},
                }
            ],
        }
        return {
            "version": "1.0.0",
            "queries": [
                {
                    "Query": {
                        "Commands": [
                            {
                                "SemanticQueryDataShapeCommand": {
                                    "Query": query,
                                    "Binding": {
                                        "Primary": {"Groupings": [{"Projections": [0, 1, 2], "Subtotal": 1}]},
                                        "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": 500}}},
                                        "Version": 1,
                                    },
                                    "ExecutionMetricsKind": 1,
                                }
                            }
                        ]
                    },
                    "QueryId": "",
                    "ApplicationContext": {
                        "DatasetId": "35b53c35-e590-4b77-8b00-b6cf403eff38",
                        "Sources": [
                            {"ReportId": "bdc9170f-1a7e-44d2-be94-98e07915d04c", "VisualId": "ecc58064d3cee7a05d02"}
                        ],
                    },
                }
            ],
            "cancelQueries": [],
            "modelId": 2256951,
            "userPreferredLocale": "en-US",
        }

    @staticmethod
    def _json_to_df(data: dict) -> pl.DataFrame:
        # TODO: Maybe also just store the "Total" number, which is in
        # data["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][1]["DM0"]

        data_array = data["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][1]["DM1"]

        # Create a list of dictionaries for each city
        rows = []
        for item in data_array:
            if "C" in item and len(item["C"]) >= 3:  # Only process items with all three values
                rows.append({
                    "municipality": item["C"][0],
                    "installed_hp_accounts": int(item["C"][1].rstrip("L")),  # Remove 'L' and convert to int
                    "installed_hp_locations": int(item["C"][2].rstrip("L")),  # Remove 'L' and convert to int
                })

        # Create DataFrame - keep municipality as a regular column
        return pl.DataFrame(rows)

    def run_query_dict(self, token: str) -> dict:
        headers = SHARED_HEADERS | {"authorization": f"EmbedToken {token}"}

        # Create and send the query
        query_payload = self._create_query()
        # print(json.dumps(query_payload, indent=4)) # debug line

        response = requests.post(self.endpoint_url, headers=headers, json=query_payload, timeout=30)

        if response.status_code == 200:
            content = response.text.encode("utf-8").decode("utf-8-sig")  # Remove BOM
            data = json.loads(content)
            # print(json.dumps(data, indent=4)) # debug line
            return data
        else:
            # todo, this should be a proper error
            print(f"Error: {response.status_code}")
            print(response.text)
            return {}

    def run_query(self, token: str) -> pl.DataFrame:
        return self.__class__._json_to_df(self.run_query_dict(token))


async def extract_auth_token():
    """
    h/t Claude
    """
    async with async_playwright() as p:
        # Launch browser using system Chromium
        browser = await p.chromium.launch(
            executable_path="/usr/bin/chromium",
            headless=True,
        )
        page = await browser.new_page()

        # Store the authorization token
        auth_token = None

        # Listen for network requests to capture the token
        async def handle_request(request):
            nonlocal auth_token
            if "querydata" in request.url.lower():
                # print(f"Found queryData request: {request.url}")
                headers = request.headers
                if "authorization" in headers:
                    auth_token = headers["authorization"]
                    print(f"Found authorization token: {auth_token[:50]}...")
                else:
                    raise ValueError(  # noqa: TRY003
                        f"No authorization header found in queryData request\nAvailable headers: {list(headers.keys())}"
                    )

        page.on("request", handle_request)

        try:
            # Navigate to the page
            print("Navigating to the page...")
            await page.goto("https://viewer.dnv.com/macustomerprofile/entity/1444/report/2078")

            # Wait for page to load (using domcontentloaded instead of networkidle)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                # print("Page loaded successfully")
            except Exception as e:
                raise ValueError("Page load timeout") from e  # noqa: TRY003

            # Wait for the queryData request to be made
            # print("Waiting for network requests...")
            await page.wait_for_timeout(15000)

            if auth_token:
                print(f"Successfully extracted authorization token: {auth_token}")
                assert "EmbedToken" in auth_token, "Authorization token does not start with EmbedToken"  # noqa: S101
                return auth_token.removeprefix("EmbedToken ")
            else:
                raise ValueError("Could not find authorization token in queryData requests")  # noqa: TRY003, TRY301

        except Exception as e:
            raise ValueError("Error") from e
        finally:
            await browser.close()


if __name__ == "__main__":
    date = datetime.now().strftime("%Y%m%d")
    outfile = f"masssave_hpinstalls_{date}.csv"

    filter_sets = {
        "Year": ["2019", "2020", "2021", "2022", "2023"],
        "Displaced_fuel": ["No displacement", "Electric", "Gas", "Oil", "Propane", "Other"],
        "End use": ["Hot Water", "HVAC"],
        "Rate_category": ["Market rate", "Income eligible"],
    }
    filter_sets = {
        "Year": ["2019"],
    }

    filter_cols = [s.lower().replace(" ", "_") for s in filter_sets]

    query_filters = []
    for col, vals in filter_sets.items():
        query_filters.append([MassSaveFilter(column=col, values=[val], operator="In") for val in vals])

    auth_token = asyncio.run(extract_auth_token())

    dfs = []
    for filter_combos in itertools.product(*query_filters):
        msq = MassSaveQuery(filters=list(filter_combos))
        # print(msq.filters) # debug line
        df = msq.run_query(auth_token)
        if df.is_empty():
            print(f"No data found for filters: {filter_combos}")
            continue
        else:
            df = df.with_columns(*[
                pl.lit(f.values[0]).alias(f.column.lower().replace(" ", "_")) for f in filter_combos
            ])
            # print(msq.filters,df["installed_hp_accounts"].sum()) # debug line
            dfs.append(df)

    data = pl.concat(dfs).sort(filter_cols).select(*[pl.col(s) for s in filter_cols], pl.all().exclude(filter_cols))
    data.write_csv(outfile)
