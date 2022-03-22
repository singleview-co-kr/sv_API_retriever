Singleview API retriever for Django
============

[![License](http://img.shields.io/badge/license-GNU%20LGPL-brightgreen.svg)](http://www.gnu.org/licenses/gpl.html)
[![Latest release](https://img.shields.io/github/v/release/singleview-co-kr/sv_API_retriever.svg)](https://github.com/singleview-co-kr/sv_API_retriever/releases)

Singleview API retriever on Python Django는 온라인 마케팅 담당자가 좀 더 쉽게 자동화된 Progrmatic Advertising 업무 단계에 접근할 수 있도록 도와줍니다.

온라인 마케팅을 책임지는 실무자가 업무와 관련된 복잡한 정보를 웹 브라우저에서 
시각적으로 확인하고 실행 개선 계획을 수립하도록 도와주는 Extraction, Transform, Load 통합 도구입니다.

이를 위해 Python Django Framework에서 Pandas, Bokeh를 이용하여 
Google Anaytics, Google Ads, Naver Ads, Facebook Ads 등에서 추출하고 표준화한 후
방대한 로우 데이터를 유연하게 시각화 가능하도록 준비해 줍니다.

AWS, Orace Cloud, Naver Cloud 등에서도 클라우드 기반의 다양한 ETL 도구를 제공하지만
매우 일반적이고 방대한 클라우드 SaaS를 온라인 마케터용 ETL 도구로 설정하는 과정은
온라인 마케팅에 책임을 지는 실무자가 실행할 수 있는 수준을 넘어섭니다.

물론, Singleview API retriever on Python Django도 관련 프레임웍에 대한 기초 지식이 전무한 상태에서 사용할 수는 없습니다.

하지만, 매우 일반적이고 방대한 클라우드 SaaS를 사용하여 데이터 분석가가 되는 노력에 비해
많은 중간 과정을 건너뛸 수 있도록 미리 구성해 놓은 통합 도구입니다.

완전한 오픈소스로 제공하여, 클라우드 SaaS와 다르게 여러가지 기능을 테스트 하다 거액의 청구서를 받고 
식은 땀을 흘리지 않아도 됩니다.

분석 플랫폼의 복잡한 데이터 파이프라인을 몰라도 쉽고 정확하게 사용할 수 있도록 
singleview.co.kr이 지난 오랜 시간 여러 프로젝트를 성공시키면서 구축한 방법론을 코드화했습니다.
오픈소스 라이선스로 누구나 사용 또는 개작할 수 있으며, 개방형 프로젝트로서 누구나 개발에 참여할 수 있습니다. 

본 프로젝트의 목표는 코딩에 관해 전문 지식이 없는 온라인 마케팅 책임자가 정량적 온라인 마케팅을 시작하려면 
즉각 부딪히는 난관을 현명하게 회피하게 하여 가능한 많은 온라인 마케팅 실무자가 Extraction, Transform, Load 
도구에 익숙해지는 것입니다.

실제로 많은 온라인 마케팅 실무자가 월간 보고서를 제작하기 위해 매체사의 로그 데이터를 엑셀로 다운받아서 
수기로 정리하느라 많은 시간과 비용을 허비합니다. 그 결과, 데이터가 나타내는 시사점에는 관심을 두지 못합니다.

위와 같은 현실을 개선하기 위해
본 프로젝트의 목표는 시중의 주요 온라인 광고 매체 운영 데이터를 API 통해 수집하고 Business Intelligence 친화적으로 재구성하는 것입니다.

노련한 개발 자원이 없는 조직이 자동화된 Progrmatic Advertising 업무 단계로 진입하는 것은 거의 불가능합니다.

이를 해결해주는 다양한 외부 서비스가 있지만, 매우 많은 비용을 지불해야 하거나, 매우 많은 비용을 지불해도 노련한 개발 자원이 없는 조직은 자사의 경영 목표에 최적화된 Business Intelligence View를 구성하지 못하는 위험성이 있습니다.

본 프로젝트는 아래의 세 가지 기준으로 이러한 문제를 해결하려고 노력합니다.

### 무료

신중한 실무자는 예산 부담을 느끼지 않고 자동화된 Progrmatic Advertising 업무 단계의 타당성 테스트를 해야 합니다.

다양한 서비스 제공자들이 이제는 무조건 자동화된 Progrmatic Advertising 업무 단계로 진입해야 한다고 주장합니다. 

관련되어 정확한 정보를 모를 때에는 전혀 부담없이 시작할 수 있을 것 같지만, 유료 서비스 제공자들과 논의를 진행할 수록 "사실은 A라는 서비스를 이용하려면 XXX를 이미 준비했어야 했다"는 뒤늦은 안내가 반복되는 경우가 있습니다.

최신 데이터 처리 기술의 낭만적이거나 이상적인 부분만 선택하여 강조한 짧은 웨비나와 같은 홍보 자료만 보면 최신 기술이 모든 오래된 문제를 해결해 줄 것 같지만,

자동화된 Progrmatic Advertising 업무 단계는 단순한 기능의 문제가 아니고 조직의 업무 처리 방식이 매우 많이 달라지는 도전이기 때문에 

숨어있던 조직 구성과 문화의 문제가 새로운 데이터 시스템과 연관되어 당황스럽게 부각될 수 있습니다. 

이러한 측면을 간과하고 성급하게 자동화된 Progrmatic Advertising 업무 단계만 도입한 조직이 갈등과 혼란만 경험한 후, 자동화된 Progrmatic Advertising 업무 단계 도입 이전보다 못한 상태로 퇴행하는 실패 사례는 셀 수 없이 많습니다.

자동화된 Progrmatic Advertising 업무 단계 도입 이전보다 못한 상태로 퇴행하는 가장 주요한 이유는 실무자들이 소중한 예산을 낭비했다는 경영진의 기술 혐오입니다.

그래서 최소한 초기 단계에서 비용이나 저작권과 같은 행정 문제를 고려하지 않고 오직 업무 타당성 검증에만 주목하여 사용해 볼 수 있어야 합니다.

### 편의성

Singleview API retriever for Python CLI proto_v1.3.0은 bash console에서만 작동했지만 Singleview API retriever on Python Django는 브라우저 환경에서 task plugin을 실행할 수 있도록 개선했습니다. 

이를 통하여 API에 대한 지식이 없는 온라인 마케터 담당자도 직접 필요한 plugin을 실행시키고 응답값을 살펴보며 브라우저에서 데이터를 수집할 수 있습니다.

다양한 매체사 API에 접속하여 수집한 대용량의 광고 운영 데이터를 이용하려는 Business Intelligence에 쉽게 업로드할 수 있도록 브라우저에서 다운로드 기능을 제공할 예정입니다. 

향후에는 온라인 마케팅 담당자의 업무에 최적화된 자체 시각화 기능을 도입할 예정입니다.

### 안정성

자동화된 Progrmatic Advertising 업무 시스템은 신뢰도가 높아야 합니다.

방대한 데이터를 수집하는 과정에서 사소한 오류가 조용히 발생하면, 사소한 오류는 이후의 데이터 재구성 과정에서 상황에 따라 확대될 수 있습니다.

왜곡된 정량 지표는 의사 결정의 오류를 발생시킬 수 있기 때문에 결코 허용해서는 안됩니다.

그래서, 실무에 사용하지 않는 자동화된 Progrmatic Advertising 업무 시스템은 무료라도 사용 가치가 거의 없습니다.

Singleview API retriever for Python CLI는 singleview.co.kr이 내부 프로젝트를 성공시키기 위한 목적으로 지난 수년간 중소대규모 온라인 마케팅 프로젝트에서 실제로 사용하여 수집 데이터의 정확성과 안정성을 확보했습니다.

### 오픈 소스 소프트웨어! 열린 프로젝트! (코드 공헌 가이드)

오픈소스 라이선스로 누구나 사용 또는 개작할 수 있으며, 개방형 프로젝트로서 누구나 개발에 참여할 수 있습니다. 

Singleview API retriever on Python Django은 singleview.co.kr이 내부 프로젝트를 성공시키기 위한 목적으로만 개발하고 사용했기 때문에 
수집 데이터의 정확성과 안정성은 확보했지만 개선의 여지는 많을 것입니다.

참여를 원하시는 분들은 버그 신고/제안 혹은 Pull Request 전에 [CONTRIBUTING.md](./CONTRIBUTING.md) 문서를 먼저 읽어주시기 바랍니다.
singleview.co.kr은 여러분들의 개발 참여를 기다립니다.

## Maintainers
@singleview-co-kr

## Contributors
온라인 마케팅 실무자가 당당하게 칼퇴근을 할 수 있는 세상을 희망하시거나
자동화된 Progrmatic Advertising 업무 시스템을 직접 개발하고 싶은 분들의 참여를 환영합니다.

## Support
* Official site (Korean) : http://singleview.co.kr/

## License
Copyright (c) 2021, singleview.co.kr <http://singleview.co.kr>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of django_etl nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.