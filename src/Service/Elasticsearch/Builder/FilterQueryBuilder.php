<?php
/**
 * Copyright (c) 2016-2017 Invertus, JSC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

namespace Invertus\Brad\Service\Elasticsearch\Builder;

use Category;
use Context;
use Configuration;
use Tools;
use Invertus\Brad\Converter\NameConverter;
use Invertus\Brad\DataType\FilterData;
use Invertus\Brad\DataType\FilterStruct;
use Invertus\Brad\Repository\CategoryRepository;
use Invertus\Brad\Util\Arrays;
use Module;
use ONGR\ElasticsearchDSL\Aggregation\Bucketing\FilterAggregation;
use ONGR\ElasticsearchDSL\Aggregation\Bucketing\RangeAggregation;
use ONGR\ElasticsearchDSL\Aggregation\Bucketing\TermsAggregation;
use ONGR\ElasticsearchDSL\BuilderInterface;
use ONGR\ElasticsearchDSL\Query\BoolQuery;
use ONGR\ElasticsearchDSL\Query\RangeQuery;
use ONGR\ElasticsearchDSL\Query\TermQuery;
use ONGR\ElasticsearchDSL\Search;

/**
 * Class FilterQueryBuilder
 *
 * @package Invertus\Brad\Service\Elasticsearch\Builder
 */
class FilterQueryBuilder extends AbstractQueryBuilder
{
    /**
     * Build filters query by given data
     *
     * @param FilterData $filterData
     * @param bool $countQuery
     *
     * @return array
     */
    public function buildFilterQuery(FilterData $filterData, $countQuery = false)
    {
        $controllerName = $filterData->getControllerName();
        $idEntity = $filterData->getIdEntity();
        $query = $this->getProductQueryBySelectedFilters(
            $filterData->getSelectedFilters(),
            $filterData->getControllerName(),
            (int)$idEntity
        );

        if ($countQuery) {
            
            return $query->toArray();
        }

        $orderBy  = $filterData->getOrderBy();
        $orderWay = $filterData->getOrderWay();
        $sort     = $this->buildOrderQuery($orderBy, $orderWay);

        $query->addSort($sort);
        $query->setFrom($filterData->getFrom());
        $query->setSize($filterData->getSize());

        // echo json_encode($query->toArray());
        return $query->toArray();
    }

    public function buildSearchFilterQuery()
    {
        $originalSearchQuery = Tools::getValue('search_query', '');
        $searchQueryString = Tools::replaceAccentedChars(urldecode($originalSearchQuery));
        return SearchQueryBuilder::buildProductsFilterQuery($searchQueryString);
    }

    /**
     * Build aggregations query
     *
     * @param FilterData $filterData
     *
     * @return array
     */
    public function buildAggregationsQuery(FilterData $filterData)
    {
        $searchQuery        = new Search();
        $filters            = $filterData->getFilters(false);
        $selectedFilters    = $filterData->getSelectedFilters();
        $hasSelectedFilters = !empty($selectedFilters);
        $idEntity           = $filterData->getIdEntity();
        $controllerName     = $filterData->getControllerName();
        // var_dump($filterData->getIdEntity());
        if (empty($filters)) {
            return [];
        }

        /** @var FilterStruct $filter */
        foreach ($filters as $filter) {
            $fieldName = NameConverter::getElasticsearchFieldName($filter->getInputName());

            if (!$hasSelectedFilters) {
                if ($controllerName == 'manufacturer') {
                    $query = $this->getQueryFromManufacturer($idEntity);
                }
                elseif ($controllerName == 'category') {
                    $query = $this->getQueryFromCategories($idEntity);
                }
                elseif ($controllerName == 'prices-drop') {
                    $query = $this->getQueryFromPricesDrop();
                }
                elseif ($controllerName == 'new-products') {
                    $query = $this->getQueryFromNewProducts();
                }
                elseif ($controllerName == 'best-sales') {
                    $query = $this->getQueryFromBestSales();
                }
                elseif ($controllerName == 'module-brad-search') {
                    $query = new BoolQuery();;
                }
                
            } else {
                $query = $this->getAggsQuery($selectedFilters, $filter->getInputName());
                if ($controllerName == 'manufacturer') {
                    $controllerQuery = $this->getQueryFromManufacturer($idEntity);
                }
                elseif ($controllerName == 'category') {
                    $controllerQuery = $this->getQueryFromCategories($idEntity);
                }
                elseif ($controllerName == 'prices-drop') {
                    $controllerQuery = $this->getQueryFromPricesDrop();
                }
                elseif ($controllerName == 'best-sales') {
                    $controllerQuery = $this->getQueryFromBestSales();
                }
                elseif ($controllerName == 'new-products') {
                    $controllerQuery = $this->getQueryFromNewProducts();
                }
                elseif ($controllerName == 'module-brad-search') {
                    // $controllerQuery = $this->getQueryFromSearch();
                }
                if (isset($controllerQuery)) {
                    $query->add($controllerQuery, BoolQuery::MUST);
                }
                
            }

            if (in_array($filter->inputName, ['price', 'weight'])) {
                $ranges = [];

                $criterias = $filter->getCriterias();
                $lastKey = Arrays::getLastKey($criterias);

                foreach ($criterias as $key => $criteria) {
                    // Simple hack to make last value inclusive
                    $extraAmount = ($lastKey == $key) ? 0.01 : 0;
                    list($from, $to) = explode(':', $criteria['value']);
                    $ranges[] = ['key' => $criteria['value'], 'from' => (float) $from, 'to' => (float) $to + $extraAmount];
                }

                $aggregation = new RangeAggregation($fieldName, $fieldName, $ranges, true);
            } else {
                $aggregation = new TermsAggregation($fieldName, $fieldName);
            }

            $filterAggregation = new FilterAggregation($fieldName, $query);
            if ($controllerName == 'module-brad-searchaaa') {
                $filterAggregation->addParameter('filter', $this->buildSearchFilterQuery());
            }
            $filterAggregation->addAggregation($aggregation);
            $searchQuery->addAggregation($filterAggregation);
        }
        // echo(json_encode($searchQuery->toArray()));
        if ($controllerName == 'module-brad-search') {

            $originalSearchQuery = Tools::getValue('search_query', '');
            $searchQueryString = Tools::replaceAccentedChars(urldecode($originalSearchQuery));
            $searchQueryArray = $searchQuery->toArray();
            $searchAggregationFilter = SearchQueryBuilder::buildProductsFilterQuery($searchQueryString);

            foreach ($searchQueryArray["aggregations"] as $key => $aggregation) {
                $searchQueryArray["aggregations"][$key]["filter"]=["bool"=>["should"=>$searchAggregationFilter]];
            }

            // echo(json_encode($searchQueryArray));
            return $searchQueryArray;
        }
        else {
            // echo json_encode($searchQuery->toArray());
            return $searchQuery->toArray();
        }
    }

    /**
     * Get aggregation query
     *
     * @param array $selectedFilters
     * @param string $aggregationInputName
     *
     * @return BoolQuery
     */
    protected function getAggsQuery($selectedFilters, $aggregationInputName)
    {
        $boolQuery = new BoolQuery();

        foreach ($selectedFilters as $name => $values) {
            if (empty($values)) {
                continue;
            }

            if ($name == $aggregationInputName) {
                continue;
            }

            if (in_array($name, ['price', 'weight'])) {
                $query = $this->getBoolShouldRangeQuery($name, $values);
            } else {
                $query = $this->getBoolShouldTermQuery($name, $values);
            }

            $boolQuery->add($query);
        }

        return $boolQuery;
    }

    /**
     * Get search values by selected filters
     *
     * @param array $selectedFilters
     * @param string $controllerName
     * @param int $idEntity
     *
     * @return Search
     */
    protected function getProductQueryBySelectedFilters(array $selectedFilters, $controllerName, $idEntity)
    {
        $searchQuery = new Search();
        $boolMustFilterQuery = new BoolQuery();
        $skipCategoriesQuery = false;
        $filters = null;
        if ($controllerName == 'module-brad-search') {
            $filters = $this->buildSearchFilterQuery();
            $boolSearchQuery = new BoolQuery();
            $boolSearchQuery->addParameter('should', $filters);
            $searchQuery->addQuery($boolSearchQuery, BoolQuery::SHOULD);
            $filters = null;
        }

        foreach ($selectedFilters as $name => $values) {
            if (0 === strpos($name, 'feature') || 0 === strpos($name, 'attribute_group')) {
                $boolShouldTermQuery = $this->getBoolShouldTermQuery($name, $values, $filters);
                $boolMustFilterQuery->add($boolShouldTermQuery);
            } elseif ('price' == $name) {
                $boolShouldRangeQuery = $this->getBoolShouldRangeQuery($name, $values, $filters);
                $boolMustFilterQuery->add($boolShouldRangeQuery);
            } elseif ('manufacturer' == $name) {
                $boolShouldTermQuery = $this->getBoolShouldTermQuery($name, $values, $filters);
                $boolMustFilterQuery->add($boolShouldTermQuery);
            } elseif ('weight' == $name) {
                $boolShouldTermQuery = $this->getBoolShouldRangeQuery($name, $values, $filters);
                $boolMustFilterQuery->add($boolShouldTermQuery);
            } elseif ('quantity' == $name) {
                $boolShouldTermQuery = $this->getBoolShouldTermQuery($name, $values, $filters);
                $boolMustFilterQuery->add($boolShouldTermQuery);
            } elseif ('category' == $name) {
                $boolShouldTermQuery = $this->getBoolShouldTermQuery($name, $values, $filters);
                $boolMustFilterQuery->add($boolShouldTermQuery);

                if (!empty($searchValues['categories'])) {
                    $skipCategoriesQuery = true;
                }
            }
        }

        if ($controllerName == 'manufacturer') {
            $termQuery = new TermQuery('id_manufacturer', $idEntity);
            $boolMustManufacturerQuery = new BoolQuery();
            $boolMustManufacturerQuery->add($termQuery, BoolQuery::MUST);
            if (!empty($boolMustFilterQuery->getQueries())) {
                $searchQuery->addQuery($boolMustFilterQuery);
            }
           $searchQuery->addQuery($this->getQueryFromManufacturer($idEntity), BoolQuery::MUST);
        }

        elseif ($controllerName == 'prices-drop') {
            $context = Context::getContext();
            $field = 'reduction_group_'.$context->customer->id_default_group.'_country_'.$context->country->id.'_currency_'.$context->currency->id;
            $termQuery = new TermQuery($field, 0);
            if (!empty($boolMustFilterQuery->getQueries())) {
                $searchQuery->addQuery($boolMustFilterQuery);
            }
           $searchQuery->addQuery($termQuery, BoolQuery::MUST_NOT);
        }

        elseif ($controllerName == 'best-sales') {
            $termQuery = new RangeQuery('number_sold', [RangeQuery::GT => 5]);
            if (!empty($boolMustFilterQuery->getQueries())) {
                $searchQuery->addQuery($boolMustFilterQuery);
            }
           $searchQuery->addQuery($termQuery, BoolQuery::MUST);
        }

        elseif ($controllerName == 'new-products') {
            $date = date('Y-m-d H:m:s', strtotime('-'.(Configuration::get('PS_NB_DAYS_NEW_PRODUCT') ? (int) Configuration::get('PS_NB_DAYS_NEW_PRODUCT') : 20).' days'));
            $termQuery = new RangeQuery('date_add', [RangeQuery::GT => $date]);
            if (!empty($boolMustFilterQuery->getQueries())) {
                $searchQuery->addQuery($boolMustFilterQuery);
            }
           $searchQuery->addQuery($termQuery, BoolQuery::MUST);
        }

        elseif ($controllerName == 'module-brad-search') {
            // $termQuery = new RangeQuery('date_add', [RangeQuery::GT => $date]);
            if (!empty($boolMustFilterQuery->getQueries())) {
                $searchQuery->addQuery($boolMustFilterQuery, BoolQuery::MUST);
            }
           // $searchQuery->addQuery($termQuery, BoolQuery::MUST);
        }

        else {
            $boolShouldCategoriesQuery = $this->getQueryFromCategories($idEntity, $controllerName);
            $boolMustCategoriesQuery   = new BoolQuery();

            if ($boolShouldCategoriesQuery instanceof BuilderInterface) {
                $boolMustCategoriesQuery->add($boolShouldCategoriesQuery);
            }

            if (!empty($boolMustFilterQuery->getQueries())) {
                $searchQuery->addQuery($boolMustFilterQuery);
            }

            if (!$skipCategoriesQuery) {
                $searchQuery->addQuery($boolMustCategoriesQuery, BoolQuery::MUST);
            }
        }

        // echo(json_encode($searchQuery->toArray()));
        return $searchQuery;
    }

    /**
     * Get subcategories query
     *
     * @param int $idCategory
     *
     * @return BoolQuery|null
     */
    protected function getQueryFromCategories($idEntity)
    {
        return $this->getBoolShouldTermQuery('category', [$idEntity]);
    }

    /**
     * Get manufacturer query
     *
     * @param int $idManufacturer
     *
     * @return BoolQuery|null
     */
    protected function getQueryFromManufacturer($idEntity)
    {
        $boolMustQuery = new BoolQuery();
        $termQuery = new TermQuery('id_manufacturer', (int)$idEntity);
        $boolMustQuery->add($termQuery, BoolQuery::MUST);

        return $boolMustQuery;
    }

    /**
     * Get prices-drop query
     *
     *
     * @return BoolQuery|null
     */
    protected function getQueryFromPricesDrop()
    {
        $context = Context::getContext();
        $field = 'reduction_group_'.$context->customer->id_default_group.'_country_'.$context->country->id.'_currency_'.$context->currency->id;
        $termQuery = new TermQuery($field, 0);
        $boolMustPricesDropQuery = new BoolQuery();
        $boolMustPricesDropQuery->add($termQuery, BoolQuery::MUST_NOT);
        return $boolMustPricesDropQuery;
    }

    /**
     * Get best-sales query
     *
     *
     * @return BoolQuery|null
     */
    protected function getQueryFromBestSales()
    {
        $termQuery = new RangeQuery('number_sold', [RangeQuery::GT => 5]);
        $boolQuery = new BoolQuery();
        $boolQuery->add($termQuery, BoolQuery::MUST);

        return $boolQuery;
    }

    /**
     * Get new products query
     *
     *
     * @return BoolQuery|null
     */
    protected function getQueryFromNewProducts()
    {
        $date = date('Y-m-d H:m:s', strtotime('-'.(Configuration::get('PS_NB_DAYS_NEW_PRODUCT') ? (int) Configuration::get('PS_NB_DAYS_NEW_PRODUCT') : 20).' days'));
        $termQuery = new RangeQuery('date_add', [RangeQuery::GT => $date]);
        $boolQuery = new BoolQuery();
        $boolQuery->add($termQuery, BoolQuery::MUST);

        return $boolQuery;
    }

    /**
     * Get search query
     *
     *
     * @return BoolQuery|null
     */
    protected function getQueryFromSearch()
    {
        $boolShouldQuery = new BoolQuery();

        return $boolShouldQuery;
    }

    protected function getSearchAggregationFilter()
    {

    }


    /**
     * Get bool should query with terms query inside
     *
     * @param string $filterIntputName
     * @param array $values
     * @param array $filters
     *
     * @return BoolQuery
     */
    protected function getBoolShouldTermQuery($filterIntputName, array $values, array $filters = null)
    {
        $fieldName = NameConverter::getElasticsearchFieldName($filterIntputName);

        $boolShouldQuery = new BoolQuery();

        foreach ($values as $value) {
            if ('category' == $filterIntputName) {
                $value = (int) $value;
            }
            $termQuery = new TermQuery($fieldName, $value);
            $boolShouldQuery->add($termQuery, BoolQuery::SHOULD);
            if ($filters) {
               $boolShouldQuery->addParameter('filter', $filters);
            }
            
        }

        return $boolShouldQuery;
    }

    /**
     * Get bool should query with ranges inside
     *
     * @param string $filterName
     * @param array $values
     * @param array $filters
     *
     * @return BoolQuery
     */
    protected function getBoolShouldRangeQuery($filterName, array $values, array $filters = null)
    {
        $fieldName = NameConverter::getElasticsearchFieldName($filterName);

        $boolShouldQuery = new BoolQuery();

        foreach ($values as $value) {
            if (empty($value)) {
                continue;
            }

            $params = [
                'gte'   => (float) $value['min_value'],
                // Simple hack to include last value
                'lt'  => (float) $value['max_value'] + 0.01,
            ];

            if ($filters) {
               $boolShouldQuery->addParameter('filter', $filters);
            }

            $rangeQuery = new RangeQuery($fieldName, $params);
            $boolShouldQuery->add($rangeQuery, BoolQuery::SHOULD);
        }

        return $boolShouldQuery;
    }

    /**
     * Get bool must term query
     *
     * @param string $filterIntputName
     * @param array $values
     * @param array $filters
     *
     * @return BoolQuery
     */
    protected function getBoolMustTermQuery($filterIntputName, $values, array $filters = null)
    {
        $fieldName = NameConverter::getElasticsearchFieldName($filterIntputName);

        $boolMustQuery = new BoolQuery();

        foreach ($values as $value) {
            $termQuery = new TermQuery($fieldName, $value);
            $boolMustQuery->add($termQuery, BoolQuery::MUST);
            if ($filters) {
               $boolShouldQuery->addParameter('filter', $filters);
            }
        }

        return $boolMustQuery;
    }
}
