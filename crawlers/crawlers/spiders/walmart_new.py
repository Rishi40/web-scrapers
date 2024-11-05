from unicodedata import category
from scrapy.selector import Selector
import scrapy
import json
import re
from worldduty.items import FactiveItem
from worldduty.common_functions import get_size_from_title, SCRAPER_URL, visited_skus, BENCHMARK_DATE, clean_product_description, get_web_page, write_to_log
from datetime import datetime
import time
from scrapy import signals
import requests

WEBSITE_ID = 40

def get_payload(sub_category_id,page_number):
    payload = {"query":"query Browse( $query:String $limit:Int $page:Int $prg:Prg! $facet:String $sort:Sort $catId:String! $max_price:String $min_price:String $module_search:String $affinityOverride:AffinityOverride $ps:Int $ptss:String $beShelfId:String $fitmentFieldParams:JSON ={}$fitmentSearchParams:JSON ={}$rawFacet:String $seoPath:String $trsp:String $fetchMarquee:Boolean! $fetchSkyline:Boolean! $fetchGallery:Boolean! $fetchSbaTop:Boolean! $fetchDac:Boolean! $additionalQueryParams:JSON ={}$enablePortableFacets:Boolean = false $enableFashionTopNav:Boolean = false $intentSource:IntentSource $tenant:String! $enableFacetCount:Boolean = true $pageType:String! = \"BrowsePage\" $marketSpecificParams:String $enableFlattenedFitment:Boolean = false $enableMultiSave:Boolean = false $fSeo:Boolean = true ){search( query:$query limit:$limit page:$page prg:$prg facet:$facet sort:$sort cat_id:$catId max_price:$max_price min_price:$min_price module_search:$module_search affinityOverride:$affinityOverride additionalQueryParams:$additionalQueryParams ps:$ps ptss:$ptss trsp:$trsp intentSource:$intentSource _be_shelf_id:$beShelfId pageType:$pageType ){query searchResult{...BrowseResultFragment}}contentLayout( channel:\"WWW\" pageType:$pageType tenant:$tenant version:\"v1\" searchArgs:{query:$query cat_id:$catId _be_shelf_id:$beShelfId prg:$prg}){modules( p13n:{page:$page userReqInfo:{refererContext:{catId:$catId}}}){...ModuleFragment configs{__typename...on EnricherModuleConfigsV1{zoneV1}...on TempoWM_GLASSWWWEmailSignUpWidgetConfigs{_rawConfigs}...on _TempoWM_GLASSWWWSearchSortFilterModuleConfigs{facetsV1 @skip(if:$enablePortableFacets){...FacetFragment}topNavFacets @include(if:$enablePortableFacets){...FacetFragment}allSortAndFilterFacets @include(if:$enablePortableFacets){...FacetFragment}}...on TempoWM_GLASSWWWPillsModuleConfigs{moduleSource pillsV2{...PillsModuleFragment}}...TileTakeOverProductFragment...on TempoWM_GLASSWWWSearchFitmentModuleConfigs{fitments( fitmentSearchParams:$fitmentSearchParams fitmentFieldParams:$fitmentFieldParams ){...FitmentFragment sisFitmentResponse{...BrowseResultFragment}}}...on TempoWM_GLASSWWWSearchACCStoreSelectionConfigs{ctaText userInfoMessage headingDetails{heading headingWhenFulfillmentIsSelectedAsPickup}}...on TempoWM_GLASSWWWStoreSelectionHeaderConfigs{fulfillmentMethodLabel storeDislayName}...on TempoWM_GLASSWWWSponsoredProductCarouselConfigs{_rawConfigs}...on TempoWM_GLASSWWWBenefitProgramBannerPlaceholderConfigs{_rawConfigs}...on TempoWM_GLASSWWWBrowseRelatedShelves @include(if:$fSeo){seoBrowseRelmData( id:$catId marketSpecificParams:$marketSpecificParams ){relm{id url name}}}...FashionTopNavFragment @include(if:$enableFashionTopNav)...BrandAmplifierAdConfigs @include(if:$fetchSbaTop)...PopularInModuleFragment...CopyBlockModuleFragment...BannerModuleFragment...HeroPOVModuleFragment...InlineSearchModuleFragment...MarqueeDisplayAdConfigsFragment @include(if:$fetchMarquee)...SkylineDisplayAdConfigsFragment @include(if:$fetchSkyline)...GalleryDisplayAdConfigsFragment @include(if:$fetchGallery)...DynamicAdContainerConfigsFragment @include(if:$fetchDac)...HorizontalChipModuleConfigsFragment...SkinnyBannerFragment}}...LayoutFragment pageMetadata{location{pickupStore deliveryStore intent postalCode stateOrProvinceCode city storeId accessPointId accessType spokeNodeId}pageContext}}seoBrowseMetaData( id:$catId facets:$rawFacet path:$seoPath facet_query_param:$facet _be_shelf_id:$beShelfId marketSpecificParams:$marketSpecificParams ){metaTitle metaDesc metaCanon h1 noIndex}}fragment BrowseResultFragment on SearchInterface{title aggregatedCount...BreadCrumbFragment...ShelfDataFragment...DebugFragment...ItemStacksFragment...PageMetaDataFragment...PaginationFragment...RequestContextFragment...ErrorResponse modules{facetsV1 @skip(if:$enablePortableFacets){...FacetFragment}topNavFacets @include(if:$enablePortableFacets){...FacetFragment}allSortAndFilterFacets @include(if:$enablePortableFacets){...FacetFragment}pills{...PillsModuleFragment}}pac{relevantPT{productType score}showPAC reasonCode}}fragment ModuleFragment on TempoModule{__typename type name version moduleId schedule{priority}matchedTrigger{zone}}fragment LayoutFragment on ContentLayout{layouts{id layout}}fragment BreadCrumbFragment on SearchInterface{breadCrumb{id name url cat_level}}fragment ShelfDataFragment on SearchInterface{shelfData{shelfName shelfId}}fragment DebugFragment on SearchInterface{debug{sisUrl adsUrl presoDebugInformation{explainerToolsResponse}}}fragment ItemStacksFragment on SearchInterface{itemStacks{displayMessage meta{adsBeacon{adUuid moduleInfo max_ads}spBeaconInfo{adUuid moduleInfo pageViewUUID placement max}query isPartialResult stackId stackType stackName title subTitle titleKey queryUsedForSearchResults layoutEnum totalItemCount totalItemCountDisplay viewAllParams{query cat_id sort facet affinityOverride recall_set min_price max_price}borderColor iconUrl}itemsV2{...ItemFragment...InGridMarqueeAdFragment...InGridAdFragment...TileTakeOverTileFragment}content{title subtitle data{type name displayName url imageUrl}}}}fragment ItemFragment on Product{__typename buyBoxSuppression similarItems id usItemId fitmentLabel name checkStoreAvailabilityATC seeShippingEligibility brand type shortDescription weightIncrement topResult additionalOfferCount imageInfo{...ProductImageInfoFragment}aspectInfo{name header id snippet}canonicalUrl externalInfo{url}itemType category{path{name url}}badges{flags{__typename...on BaseBadge{key bundleId @include(if:$enableMultiSave) text type id styleId}...on PreviouslyPurchasedBadge{id text key lastBoughtOn numBought}}tags{__typename...on BaseBadge{key text type}}groups{__typename name members{...on BadgeGroupMember{__typename id key memberType otherInfo{moqText}rank slaText styleId text type templates{regular faster unavailable}badgeContent{type value id}}...on CompositeGroupMember{__typename join memberType styleId suffix members{__typename id key memberType rank slaText styleId text type}}}}}classType averageRating numberOfReviews esrb mediaRating salesUnitType sellerId sellerName hasSellerBadge isEarlyAccessItem earlyAccessEvent annualEvent annualEventV2 availabilityStatusV2{display value}groupMetaData{groupType groupSubType numberOfComponents groupComponents{quantity offerId componentType productDisplayName}}productLocation{displayValue aisle{zone aisle}}fulfillmentSpeed offerId preOrder{...PreorderFragment}pac{showPAC reasonCode fulfillmentPacModule}fulfillmentSummary{storeId deliveryDate}priceInfo{...ProductPriceInfoFragment}variantCriteria{...VariantCriteriaFragment}snapEligible fulfillmentTitle fulfillmentType brand manufacturerName showAtc sponsoredProduct{spQs clickBeacon spTags viewBeacon}showOptions showBuyNow quickShop quickShopCTALabel rewards{eligible state minQuantity rewardAmt promotionId selectionToken rewardMultiplierStr cbOffer term expiry description}promoDiscount{discount discountEligible discountEligibleVariantPresent promotionId promoOffer state}arExperiences{isARHome isZeekit isAROptical}eventAttributes{...ProductEventAttributesFragment}subscription{subscriptionEligible}hasCarePlans petRx{eligible singleDispense}vision{ageGroup visionCenterApproved}showExploreOtherConditionsCTA isPreowned pglsCondition newConditionProductId preownedCondition keyAttributes{displayEnum value}}fragment ProductImageInfoFragment on ProductImageInfo{id name thumbnailUrl size}fragment ProductPriceInfoFragment on ProductPriceInfo{priceRange{minPrice maxPrice priceString}currentPrice{...ProductPriceFragment priceDisplay}comparisonPrice{...ProductPriceFragment}wasPrice{...ProductPriceFragment}unitPrice{...ProductPriceFragment}listPrice{...ProductPriceFragment}savingsAmount{...ProductSavingsFragment}shipPrice{...ProductPriceFragment}subscriptionPrice{priceString subscriptionString}priceDisplayCodes{priceDisplayCondition finalCostByWeight submapType}wPlusEarlyAccessPrice{memberPrice{...ProductPriceFragment}savings{...ProductSavingsFragment}eventStartTime eventStartTimeDisplay}subscriptionDualPrice subscriptionPercentage}fragment PreorderFragment on PreOrder{isPreOrder preOrderMessage preOrderStreetDateMessage streetDate streetDateDisplayable streetDateType}fragment ProductPriceFragment on ProductPrice{price priceString variantPriceString priceType currencyUnit priceDisplay}fragment ProductSavingsFragment on ProductSavings{amount percent priceString}fragment ProductEventAttributesFragment on EventAttributes{priceFlip specialBuy}fragment VariantCriteriaFragment on VariantCriterion{name type id displayName isVariantTypeSwatch variantList{id images name rank swatchImageUrl availabilityStatus products selectedProduct{canonicalUrl usItemId}}}fragment InGridMarqueeAdFragment on MarqueePlaceholder{__typename type moduleLocation lazy}fragment InGridAdFragment on AdPlaceholder{__typename type moduleLocation lazy adUuid hasVideo moduleInfo}fragment TileTakeOverTileFragment on TileTakeOverProductPlaceholder{__typename type tileTakeOverTile{span title subtitle image{src alt assetId assetName}logoImage{src alt}backgroundColor titleTextColor subtitleTextColor tileCta{ctaLink{clickThrough{value}linkText title}ctaType ctaTextColor}adsEnabled adCardLocation enableLazyLoad}}fragment PageMetaDataFragment on SearchInterface{pageMetadata{storeSelectionHeader{fulfillmentMethodLabel storeDislayName}title canonical description location{addressId}subscriptionEligible}}fragment PaginationFragment on SearchInterface{paginationV2{maxPage pageProperties}}fragment RequestContextFragment on SearchInterface{requestContext{vertical hasGicIntent isFitmentFilterQueryApplied searchMatchType categories{id name}}}fragment ErrorResponse on SearchInterface{errorResponse{correlationId source errorCodes errors{errorType statusCode statusMsg source}}}fragment PillsModuleFragment on PillsSearchInterface{title titleColor url image:imageV1{src alt assetId assetName}}fragment BannerViewConfigFragment on BannerViewConfigCLS{title image imageAlt displayName description url urlAlt appStoreLink appStoreLinkAlt playStoreLink playStoreLinkAlt}fragment BannerModuleFragment on TempoWM_GLASSWWWSearchBannerConfigs{moduleType viewConfig{...BannerViewConfigFragment}}fragment PopularInModuleFragment on TempoWM_GLASSWWWPopularInBrowseConfigs{seoBrowseRelmData(id:$catId){relm{id name url}}}fragment CopyBlockModuleFragment on TempoWM_GLASSWWWCopyBlockConfigs{copyBlock(id:$catId marketSpecificParams:$marketSpecificParams){cwc}}fragment FacetFragment on Facet{title name expandOnLoad type displayType layout min max selectedMin selectedMax unboundedMax stepSize isSelected values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL}}}}}}}}}fragment FitmentFragment on Fitments{partTypeIDs isNarrowSearch fitmentOptionalFields{...FitmentFieldFragment}result{status formId position quantityTitle extendedAttributes{...FitmentFieldFragment}labels{...LabelFragment}resultSubTitle notes suggestions{...FitmentSuggestionFragment}}redirectUrl{title clickThrough{value}}labels{...LabelFragment}savedVehicle{vehicleType{...VehicleFieldFragment}vehicleYear{...VehicleFieldFragment}vehicleMake{...VehicleFieldFragment}vehicleModel{...VehicleFieldFragment}additionalAttributes{...VehicleFieldFragment}}fitmentFields{...VehicleFieldFragment}fitmentForms{id fields{...FitmentFieldFragment}title labels{...LabelFragment}garage{vehicles{...AutoVehicle}}}}fragment LabelFragment on FitmentLabels{ctas{...FitmentLabelEntityFragment}messages{...FitmentLabelEntityFragment}links{...FitmentLabelEntityFragment}images{...FitmentLabelEntityFragment}}fragment FitmentLabelEntityFragment on FitmentLabelEntity{id label labelV1 @include(if:$enableFlattenedFitment)}fragment VehicleFieldFragment on FitmentVehicleField{id label value}fragment FitmentFieldFragment on FitmentField{id displayName value extended data{value label}dependsOn isRequired errorMessage}fragment FitmentSuggestionFragment on FitmentSuggestion{id position loadIndex speedRating searchQueryParam labels{...LabelFragment}cat_id fitmentSuggestionParams{id value}optionalSuggestionParams{id data{label value}}}fragment HeroPOVModuleFragment on TempoWM_GLASSWWWHeroPovConfigsV1{povCards{card:cardV1{povStyle image{mobileImage{...TempoCommonImageFragment}desktopImage{...TempoCommonImageFragment}}heading{text textColor textSize}subheading{text textColor}detailsView{backgroundColor isTransparent}ctaButton{button{linkText clickThrough{value}uid}ctaButtonBackgroundColor textColor}legalDisclosure{regularText shortenedText textColor textColorMobile legalBottomSheetTitle legalBottomSheetDescription}logo{...TempoCommonImageFragment}links{link{...TempoCommonLinkFragment}textColor textColorMobile}}}}fragment TempoCommonImageFragment on TempoCommonImage{src alt assetId uid clickThrough{value}}fragment TempoCommonLinkFragment on TempoCommonStringLink{linkText title uid clickThrough{value}}fragment InlineSearchModuleFragment on TempoWM_GLASSWWWInlineSearchConfigs{headingText placeholderText}fragment MarqueeDisplayAdConfigsFragment on TempoWM_GLASSWWWMarqueeDisplayAdConfigs{_rawConfigs ad{...DisplayAdFragment}}fragment DisplayAdFragment on Ad{...AdFragment adContent{type data{__typename...AdDataDisplayAdFragment}}}fragment AdFragment on Ad{status moduleType platform pageId pageType storeId stateCode zipCode pageContext moduleConfigs adsContext adRequestComposite}fragment AdDataDisplayAdFragment on AdData{...on DisplayAd{json status}}fragment SkylineDisplayAdConfigsFragment on TempoWM_GLASSWWWSkylineDisplayAdConfigs{_rawConfigs ad{...SkylineDisplayAdFragment}}fragment SkylineDisplayAdFragment on Ad{...SkylineAdFragment adContent{type data{__typename...SkylineAdDataDisplayAdFragment}}}fragment SkylineAdFragment on Ad{status moduleType platform pageId pageType storeId stateCode zipCode pageContext moduleConfigs adsContext adRequestComposite}fragment SkylineAdDataDisplayAdFragment on AdData{...on DisplayAd{json status}}fragment GalleryDisplayAdConfigsFragment on TempoWM_GLASSWWWGalleryDisplayAdConfigs{_rawConfigs}fragment DynamicAdContainerConfigsFragment on TempoWM_GLASSWWWDynamicAdContainerConfigs{_rawConfigs adModules{moduleType moduleLocation priority adServers{adServer}}zoneLocation lazy}fragment HorizontalChipModuleConfigsFragment on TempoWM_GLASSWWWHorizontalChipModuleConfigs{chipModuleSource:moduleSource heading headingColor backgroundImage{src alt}backgroundColor desktopImageHeight desktopImageWidth mobileImageHeight mobileImageWidth chipModule{title url{linkText title clickThrough{type value}}}chipModuleWithImages{title titleColor url{linkText title clickThrough{type value}}image{assetId assetName alt clickThrough{type value}height src title width}}}fragment SkinnyBannerFragment on TempoWM_GLASSWWWSkinnyBannerConfigs{campaignsV1{bannerType desktopBannerHeight bannerImage{src title alt assetId assetName}mobileBannerHeight mobileImage{src title alt assetId assetName}clickThroughUrl{clickThrough{value}}backgroundColor heading{title fontColor}subHeading{title fontColor}bannerCta{ctaLink{linkText clickThrough{value}}textColor ctaType}}}fragment TileTakeOverProductFragment on TempoWM_GLASSWWWTileTakeOverProductConfigs{dwebSlots mwebSlots overrideDefaultTiles TileTakeOverProductDetailsV1{pageNumber span dwebPosition mwebPosition title subtitle image{src alt assetId assetName}logoImage{src alt}backgroundColor titleTextColor subtitleTextColor tileCta{ctaLink{clickThrough{value}linkText title uid}ctaType ctaTextColor}adsEnabled adCardLocation enableLazyLoad}}fragment FashionTopNavFragment on TempoWM_GLASSWWWCategoryTopNavConfigs{navHeaders{header{linkText clickThrough{value}}headerImageGroup{headerImage{alt src assetId assetName}imgTitle imgSubText imgLink{linkText title clickThrough{value}}}categoryGroup{category{linkText clickThrough{value}}startNewColumn subCategoryGroup{subCategory{linkText clickThrough{value}}isBold openInNewTab}}}}fragment BrandAmplifierAdConfigs on TempoWM_GLASSWWWBrandAmplifierAdConfigs{_rawConfigs moduleLocation ad{...SponsoredBrandsAdFragment}}fragment SponsoredBrandsAdFragment on Ad{...AdFragment adContent{type data{__typename...AdDataSponsoredBrandsFragment}}}fragment AdDataSponsoredBrandsFragment on AdData{...on SponsoredBrands{adUuid adExpInfo moduleInfo brands{logo{featuredHeadline featuredImage featuredImageName featuredUrl logoClickTrackUrl}products{...ProductFragment}}}}fragment ProductFragment on Product{usItemId offerId badges{flags{__typename...on BaseBadge{id text key query type styleId}...on PreviouslyPurchasedBadge{id text key lastBoughtOn numBought criteria{name value}}}labels{__typename...on BaseBadge{id text key}...on PreviouslyPurchasedBadge{id text key lastBoughtOn numBought}}tags{__typename...on BaseBadge{id text key}}groups{__typename name members{...on BadgeGroupMember{__typename id key memberType rank slaText styleId text type}...on CompositeGroupMember{__typename join memberType styleId suffix members{__typename id key memberType rank slaText styleId text type}}}}}priceInfo{priceDisplayCodes{rollback reducedPrice eligibleForAssociateDiscount clearance strikethrough submapType priceDisplayCondition unitOfMeasure pricePerUnitUom}currentPrice{price priceString priceDisplay}wasPrice{price priceString}listPrice{price priceString}priceRange{minPrice maxPrice priceString}unitPrice{price priceString}savingsAmount{priceString}comparisonPrice{priceString}subscriptionPrice{priceString subscriptionString price minPrice maxPrice intervalFrequency duration percentageRate durationUOM interestUOM}wPlusEarlyAccessPrice{memberPrice{price priceString priceDisplay}savings{amount priceString}eventStartTime eventStartTimeDisplay}}annualEventV2 earlyAccessEvent isEarlyAccessItem eventAttributes{priceFlip specialBuy}snapEligible showOptions sponsoredProduct{spQs clickBeacon spTags}canonicalUrl numberOfReviews averageRating availabilityStatus imageInfo{thumbnailUrl allImages{id url}}name fulfillmentBadge classType type showAtc p13nData{predictedQuantity flags{PREVIOUSLY_PURCHASED{text}CUSTOMERS_PICK{text}}labels{PREVIOUSLY_PURCHASED{text}CUSTOMERS_PICK{text}}}brand}fragment AutoVehicle on AutoVehicle{cid color default documentType fitment{baseBodyType baseVehicleId engineOptions{id isSelected label}smartSubModel tireSizeOptions{diameter isCustom isSelected loadIndex positions ratio speedRating tirePressureFront tirePressureRear tireSize width}trim}isDually licensePlate licensePlateState make model source sourceType subModel{subModelId subModelName}subModelOptions{subModelId subModelName}vehicleId vehicleType vin year}","variables":{"id":"","dealsId":"","query":"","page":page_number,"prg":"desktop","catId":sub_category_id,"facet":"","sort":"best_match","rawFacet":"","seoPath":"","ps":40,"limit":40,"ptss":"","trsp":"","beShelfId":"","recall_set":"","module_search":"","min_price":"","max_price":"","storeSlotBooked":"","additionalQueryParams":{"hidden_facet":None,"translation":None,"isMoreOptionsTileEnabled":True},"searchArgs":{"query":"","cat_id":sub_category_id,"prg":"desktop","facet":""},"fitmentFieldParams":{"powerSportEnabled":True,"dynamicFitmentEnabled":False,"extendedAttributesEnabled":False},"fitmentSearchParams":{"id":"","dealsId":"","query":"","page":page_number,"prg":"desktop","catId":sub_category_id,"facet":"","sort":"best_match","rawFacet":"","seoPath":"","ps":40,"limit":40,"ptss":"","trsp":"","beShelfId":"","recall_set":"","module_search":"","min_price":"","max_price":"","storeSlotBooked":"","additionalQueryParams":{"hidden_facet":None,"translation":None,"isMoreOptionsTileEnabled":True},"searchArgs":{"query":"","cat_id":sub_category_id,"prg":"desktop","facet":""},"cat_id":sub_category_id,"_be_shelf_id":""},"enableFashionTopNav":False,"fetchMarquee":True,"fetchSkyline":True,"fetchSbaTop":False,"fetchGallery":False,"fetchDac":False,"enablePortableFacets":True,"tenant":"CA_GLASS","enableFacetCount":True,"marketSpecificParams":"{\"pageType\":\"browse\"}","enableFlattenedFitment":False,"enableMultiSave":True,"fSeo":True,"pageType":"BrowsePage"}}

    return payload

def get_header(sub_category,sub_category_id):
    catalog_header = {
        'authority': 'www.walmart.ca',
        'accept': 'application/json',
        'accept-language': 'en-CA',
        'content-type': 'application/json',
        'origin': 'https://www.walmart.ca',
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'wm_mp': 'true',
        'wm_page_url': f'https://www.walmart.ca/en/browse/grocery/snacks-candy/{sub_category}/{sub_category_id}',
        'x-apollo-operation-name': 'Browse',
        'x-enable-server-timing': '1',
        'x-latency-trace': '1',
        'x-o-bu': 'WALMART-CA',
        'x-o-ccm': 'server',
        'x-o-gql-query': 'query Browse',
    }

    return catalog_header

sub_category_id_map = {
    'chips-salty-snacks': '10019_6000194328523_6000194329540',
    'chocolate': '10019_6000194328523_6000194329539',
    'candy-gum': '10019_6000194328523_6000194329534',
    'crackers': '10019_6000194328523_6000194329538',
    'cookies': '10019_6000194328523_6000194329541',
    'fruit-snacks': '10019_6000194328523_6000194329543'
}

class WdcSpider(scrapy.Spider):
    name = "scrape_walmart"
    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'ITEM_PIPELINES': {
            'worldduty.pipelines.FactivePipeline': 300,
        },
    }

    def start_requests(self):

        category = self.category
        sub_category = self.sub_category
        sub_category_id = sub_category_id_map[sub_category]
 
        visited_sku_list = visited_skus(WEBSITE_ID,BENCHMARK_DATE,sub_category)
        page_start = 1

        url = f"https://www.walmart.ca/orchestra/graphql/browse?page={page_start}&prg=desktop&catId={sub_category_id}&sort=best_match&ps=40&limit=40"

        page_payload = get_payload(sub_category_id,page_start)
        page_header = get_header(sub_category,sub_category_id)

        yield scrapy.Request(
            method="POST",
            url=url, 
            callback=self.parse_catalogue_pages,
            headers=page_header,
            body=json.dumps(page_payload),
            meta = {
                'visited_sku_list':visited_sku_list,
                'sub_category':sub_category,
                'sub_category_id':sub_category_id,
                'page_start': page_start
            },
            dont_filter=True
        )
        
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WdcSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        stats = spider.crawler.stats.get_stats()
        spider.crawler.stats.set_value('sub_category', self.sub_category)
        log_file_name = f"{spider.name}_log.txt"
        write_to_log(log_file_name,spider.name,stats)

    def parse_catalogue_pages(self, response):
        page_start = response.meta['page_start']
        sub_category = response.meta['sub_category']
        sub_category_id = response.meta['sub_category_id']
        visited_sku_list = response.meta['visited_sku_list']

        try:
            catalogue_data_string = response.body
            catalogue_data_json = json.loads(catalogue_data_string)
        except:
            catalogue_data_json = {}

        if catalogue_data_json:
            total_pages = catalogue_data_json.get('data').get('search').get('searchResult').get('paginationV2').get('maxPage')
            print("Total Pages ---->",total_pages)
            # total_pages = 2

            for page_index in range(page_start,total_pages+1):

                catalogue_links_url = f"https://www.walmart.ca/orchestra/graphql/browse?page={page_index}&prg=desktop&catId={sub_category_id}&sort=best_match&ps=40&limit=40"

                page_payload = get_payload(sub_category_id,page_index)
                page_header = get_header(sub_category,sub_category_id)

                yield scrapy.Request(
                    method="POST",
                    url=catalogue_links_url, 
                    callback=self.parse_catalogue_links,
                    headers=page_header,
                    body=json.dumps(page_payload),
                    meta = {
                        'visited_sku_list':visited_sku_list,
                        'sub_category':sub_category,
                        'sub_category_id':sub_category_id
                    },
                    dont_filter=True
                )

    def parse_catalogue_links(self, response):
        try:
            catalogue_data_string = response.body
            catalogue_data_json = json.loads(catalogue_data_string)
        except:
            catalogue_data_json = {}

        if catalogue_data_json:
            products = catalogue_data_json.get('data').get('search').get('searchResult').get('itemStacks')[0].get('itemsV2')
            for product in products:
                item = FactiveItem()
                scrape_date = datetime.today().strftime('%Y-%m-%d')
                miscellaneous = {}

                try:
                    brand = product.get('brand')
                except:
                    brand = None

                try:
                    sku_id = product.get('usItemId')
                except Exception as e:
                    sku_id = None

                try:
                    product_name = product.get('name')
                except:
                    product_name = None

                try:
                    product_id = product.get('id')
                    if product_id:
                        miscellaneous['product_id'] = product_id
                except:
                    product_id = None

                try:
                    product_link = 'https://www.walmart.ca' + product.get('canonicalUrl')
                except:
                    product_link = None

                try:
                    image_url = product.get('imageInfo').get('thumbnailUrl')
                except Exception as e:
                    image_url = None

                try:
                    oos_string = product.get('availabilityStatusV2').get('value')
                    if oos_string.upper() == 'IN_STOCK':
                        oos = 0
                    else:
                        oos = 1
                except:
                    oos = 0

                try:
                    rating = product.get('averageRating')
                    if rating:
                        miscellaneous['rating'] = rating
                except:
                    rating = None

                try:
                    reviewCount = product.get('numberOfReviews')
                    if reviewCount:
                        miscellaneous['reviewCount'] = reviewCount
                except:
                    reviewCount = None

                try:
                    price = product.get('priceInfo').get('currentPrice').get('price')
                except:
                    price = None

                try:
                    mrp = product.get('priceInfo').get('listPrice').get('price')
                except:
                    mrp = None

                try:
                    product_description = product.get('shortDescription')
                except Exception as e:
                    product_description = None

                size = get_size_from_title(product_name)

                miscellaneous_string = json.dumps(miscellaneous)

                item['website_id'] = WEBSITE_ID
                item['scrape_date'] = scrape_date
                item['category'] = self.category
                item['sub_category'] = self.sub_category
                item['brand'] = brand
                item['sku_id'] = sku_id
                item['product_name'] = product_name
                item['product_url'] = product_link
                item['image_url'] = image_url
                item['product_description'] = product_description
                item['info_table'] = None
                item['high_street_price'] = None
                item['size'] = size
                item['qty_left'] = None
                item['usd_price'] = None
                item['usd_mrp'] = None
                item['miscellaneous'] = miscellaneous_string
                item['price'] = price
                item['mrp'] = mrp
                item['discount'] = None
                item['out_of_stock'] = oos

                yield item