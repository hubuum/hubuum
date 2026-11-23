use super::*;
use rstest::rstest;

#[rstest]
fn regex_operands_are_validated_before_storage(
    #[values(
        "name",
        "groupname",
        "json_data",
        "json_schema",
        "from_json_data",
        "computed.shared.value",
        "related.room.object.json_data"
    )]
    field: &str,
    #[values(false, true)] negated: bool,
) {
    let field = FilterField::from_str(field).unwrap();
    for pattern in hubuum_regex::conformance::invalid_patterns() {
        let value = if field_has_json_operand(&field) {
            format!("value={pattern}")
        } else {
            pattern.to_string()
        };
        assert!(
            ParsedQueryParam::from_parts(
                field.clone(),
                SearchOperator::Regex {
                    is_negated: negated
                },
                value
            )
            .is_err(),
            "{field}: {pattern}"
        );
    }
}

#[rstest]
#[case::empty("name__regex=", "")]
#[case::json_equals("json_data__regex=value=a=b", "a=b")]
#[case::json_empty("json_data__not_regex=value=", "")]
fn parsed_regex_retains_only_the_pattern_operand(#[case] query: &str, #[case] source: &str) {
    let options = parse_query_parameter(query).unwrap();
    assert_eq!(
        options.filters()[0].regex_pattern(),
        Some(&Pattern::new(source).unwrap())
    );
}

#[test]
fn related_json_remapping_preserves_the_validated_pattern() {
    let parameter = ParsedQueryParam::new(
        "related.room.object.json_data",
        Some(SearchOperator::Regex { is_negated: false }),
        "value=a=b",
    )
    .unwrap();
    let remapped = parameter.clone().with_field(FilterField::JsonData).unwrap();
    assert_eq!(remapped.regex_pattern(), parameter.regex_pattern());
}

#[test]
fn regex_remapping_cannot_change_operand_shape() {
    let parameter = ParsedQueryParam::new(
        "name",
        Some(SearchOperator::Regex { is_negated: false }),
        "value=a=b",
    )
    .unwrap();
    assert!(parameter.with_field(FilterField::JsonData).is_err());
}

#[test]
fn filter_field_keeps_ordinary_variants_compact() {
    assert!(
        std::mem::size_of::<FilterField>() <= 2 * std::mem::size_of::<usize>(),
        "FilterField should keep computed sort state behind indirection"
    );
}

#[test]
fn parses_filters_sort_cursor_and_limit() {
    let parsed = parse_query_parameter(
        "name__not_icontains=archived&limit=10&cursor=abc&sort=-created_at,name.asc",
    )
    .unwrap();

    assert_eq!(parsed.filters.len(), 1);
    assert_eq!(parsed.limit, Some(10));
    assert_eq!(parsed.cursor().map(QueryCursor::as_str), Some("abc"));
    assert_eq!(parsed.sort.len(), 2);
    assert!(parsed.sort[0].descending);
    assert!(!parsed.sort[1].descending);
}

#[test]
fn rejected_cursor_update_preserves_the_previous_valid_cursor() {
    let mut options = QueryOptions::new(
        Vec::new(),
        Vec::new(),
        None,
        Some("valid".to_string()),
        true,
    )
    .unwrap();

    let error = options
        .set_cursor(Some("x".repeat(MAX_ENCODED_CURSOR_BYTES + 1)))
        .unwrap_err();

    assert!(error.to_string().contains("maximum encoded size"));
    assert_eq!(options.cursor().map(QueryCursor::as_str), Some("valid"));
}

#[test]
fn query_options_reject_zero_and_native_limit_overflow() {
    let zero = QueryOptions::new(Vec::new(), Vec::new(), Some(0), None, false).unwrap_err();
    assert!(zero.to_string().contains("greater than 0"));

    if let Ok(overflow) = usize::try_from(i64::MAX).map(|value| value.saturating_add(1)) {
        let error =
            QueryOptions::new(Vec::new(), Vec::new(), Some(overflow), None, false).unwrap_err();
        assert!(error.to_string().contains("supported range"));
    }
}

#[test]
fn rejected_limit_update_preserves_the_previous_valid_limit() {
    let mut options = QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, false).unwrap();

    options.set_limit(Some(0)).unwrap_err();

    assert_eq!(options.limit(), Some(10));
}

#[test]
fn rejected_filter_push_preserves_the_previous_valid_filters() {
    let mut options = QueryOptions::new(
        std::iter::repeat_with(|| {
            ParsedQueryParam::from_parts(
                FilterField::Name,
                SearchOperator::Contains { is_negated: false },
                "value",
            )
            .unwrap()
        })
        .take(MAX_QUERY_FILTERS)
        .collect(),
        Vec::new(),
        None,
        None,
        true,
    )
    .unwrap();

    let error = options
        .filters_mut()
        .try_push(
            ParsedQueryParam::from_parts(
                FilterField::Description,
                SearchOperator::Contains { is_negated: false },
                "extra",
            )
            .unwrap(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("at most"));
    assert_eq!(options.filters().len(), MAX_QUERY_FILTERS);
}

#[test]
fn rejected_filter_retain_preserves_related_group_invariants() {
    let (mut options, _) = parse_query_parameter_with_computed_and_related_filters_and_passthrough(
        "related.room.class.name=Room&related.room.object.name=router",
        &[],
    )
    .unwrap();
    let before = options.filters().clone();

    let error = options
        .filters_mut()
        .try_retain(|filter| {
            !matches!(
                filter.field.related_query().map(RelatedQueryField::target),
                Some(RelatedFilterTarget::Class(RelatedClassField::Name))
            )
        })
        .unwrap_err();

    assert!(error.to_string().contains("exactly one"));
    assert_eq!(options.filters(), &before);
}

#[test]
fn parses_named_related_filter_groups() {
    let (parsed, _) =
        parse_query_parameter_with_computed_and_related_filters_and_passthrough(
            "related.room.class.name=Room&related.room.object.name__iequals=foo&related.room.depth__lte=5&related.owner.class.id=42&related.owner.object.revision__gt=4",
            &[],
        )
        .unwrap();

    assert_eq!(parsed.filters.len(), 5);
    let room = parsed.filters[0].field.related_query().unwrap();
    assert_eq!(room.alias(), "room");
    assert_eq!(
        room.target(),
        RelatedFilterTarget::Class(RelatedClassField::Name)
    );
    assert_eq!(
        parsed.filters[1].field.related_query().unwrap().target(),
        RelatedFilterTarget::Object(RelatedObjectField::Name)
    );
    assert_eq!(
        parsed.filters[2].field.related_query().unwrap().target(),
        RelatedFilterTarget::Depth
    );
}

fn related_parse_error(query: &str) -> QueryError {
    parse_query_parameter_with_computed_and_related_filters_and_passthrough(query, &[]).unwrap_err()
}

#[test]
fn related_filter_group_requires_a_class_selector() {
    let error = related_parse_error("related.room.object.name=foo");

    assert!(
        error
            .to_string()
            .contains("exactly one class.id or class.name")
    );
}

#[test]
fn related_filter_group_rejects_multiple_class_selectors() {
    let error = related_parse_error("related.room.class.name=Room&related.room.class.id=2");

    assert!(
        error
            .to_string()
            .contains("exactly one class.id or class.name")
    );
}

#[test]
fn related_filters_enforce_the_group_count_limit() {
    let groups = (0..=MAX_RELATED_FILTER_GROUPS)
        .map(|index| format!("related.g{index}.class.name=Class{index}"))
        .collect::<Vec<_>>()
        .join("&");
    let error = related_parse_error(&groups);

    assert!(
        error
            .to_string()
            .contains("at most 4 related filter groups")
    );
}

#[test]
fn related_filter_depth_rejects_zero() {
    let error = related_parse_error("related.room.class.name=Room&related.room.depth__lte=0");

    assert!(error.to_string().contains("integer from 1 to 10"));
}

#[test]
fn related_filter_depth_rejects_values_above_the_limit() {
    let error = related_parse_error("related.room.class.name=Room&related.room.depth__lte=11");

    assert!(error.to_string().contains("integer from 1 to 10"));
}

#[test]
fn related_filter_depth_rejects_non_integer_values() {
    let error = related_parse_error("related.room.class.name=Room&related.room.depth__lte=x");

    assert!(error.to_string().contains("integer from 1 to 10"));
}

#[test]
fn related_filter_rejects_an_invalid_alias() {
    assert!(
        related_parse_error("related.1room.class.name=Room")
            .to_string()
            .contains("alias")
    );
}

#[test]
fn related_filter_rejects_an_unknown_class_field() {
    let error = related_parse_error("related.room.class.description=Room");

    assert!(error.to_string().contains("Invalid related search field"));
}

#[test]
fn related_filter_class_selector_requires_equality() {
    let error = related_parse_error("related.room.class.name__icontains=Room");

    assert!(error.to_string().contains("unnegated equality"));
}

#[test]
fn related_filter_depth_requires_the_lte_suffix() {
    let error = related_parse_error("related.room.class.name=Room&related.room.depth=2");

    assert!(error.to_string().contains("only supports depth__lte"));
}

#[test]
fn related_filter_depth_rejects_other_operators() {
    let error = related_parse_error("related.room.class.name=Room&related.room.depth__gte=2");

    assert!(error.to_string().contains("only supports depth__lte"));
}

#[test]
fn standard_parser_rejects_related_filters() {
    let error = parse_query_parameter("related.room.class.name=Room").unwrap_err();

    assert!(error.to_string().contains("not supported"));
}

#[test]
fn parser_rejects_related_sort_fields() {
    let error = parse_query_parameter("sort=related.room.object.name").unwrap_err();

    assert!(error.to_string().contains("cannot be used for sorting"));
}

#[test]
fn related_filter_alias_may_contain_double_underscores() {
    let (parsed, _) = parse_query_parameter_with_computed_and_related_filters_and_passthrough(
        "related.room__west.class.name=Room&related.room__west.object.name__contains=foo",
        &[],
    )
    .unwrap();

    assert_eq!(
        parsed.filters[0].field.related_query().unwrap().alias(),
        "room__west"
    );
    assert_eq!(
        parsed.filters[1].operator,
        SearchOperator::Contains { is_negated: false }
    );
}

#[test]
fn common_parser_rejects_more_than_the_parameter_limit() {
    let query = std::iter::repeat_n("local=value", MAX_QUERY_PARAMETERS + 1)
        .collect::<Vec<_>>()
        .join("&");

    let error = parse_query_parameter_with_passthrough(&query, &["local"]).unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest(format!(
            "query accepts at most {MAX_QUERY_PARAMETERS} parameters"
        ))
    );
}

#[test]
fn common_parser_accepts_the_parameter_limit() {
    let query = std::iter::repeat_n("local=value", MAX_QUERY_PARAMETERS)
        .collect::<Vec<_>>()
        .join("&");

    let (_, passthrough) = parse_query_parameter_with_passthrough(&query, &["local"]).unwrap();

    assert_eq!(passthrough["local"].len(), MAX_QUERY_PARAMETERS);
}

#[test]
fn common_parser_rejects_more_than_the_filter_limit() {
    let query = std::iter::repeat_n("name__contains=value", MAX_QUERY_FILTERS + 1)
        .collect::<Vec<_>>()
        .join("&");

    let error = parse_query_parameter(&query).unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest(format!("query accepts at most {MAX_QUERY_FILTERS} filters"))
    );
}

#[test]
fn common_parser_accepts_the_filter_limit() {
    let query = std::iter::repeat_n("name__contains=value", MAX_QUERY_FILTERS)
        .collect::<Vec<_>>()
        .join("&");

    let parsed = parse_query_parameter(&query).unwrap();

    assert_eq!(parsed.filters.len(), MAX_QUERY_FILTERS);
}

#[test]
fn common_parser_rejects_more_than_the_sort_limit() {
    let fields = [
        "id",
        "name",
        "description",
        "created_at",
        "updated_at",
        "collection_id",
        "class_id",
        "username",
        "proper_name",
    ];
    assert_eq!(fields.len(), MAX_QUERY_SORT_FIELDS + 1);

    let error = parse_query_parameter(&format!("sort={}", fields.join(","))).unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest(format!(
            "query accepts at most {MAX_QUERY_SORT_FIELDS} sort fields"
        ))
    );
}

#[test]
fn common_parser_accepts_the_sort_limit() {
    let fields = [
        "id",
        "name",
        "description",
        "created_at",
        "updated_at",
        "collection_id",
        "class_id",
        "username",
    ];
    assert_eq!(fields.len(), MAX_QUERY_SORT_FIELDS);

    let parsed = parse_query_parameter(&format!("sort={}", fields.join(","))).unwrap();

    assert_eq!(parsed.sort.len(), MAX_QUERY_SORT_FIELDS);
}

#[test]
fn common_parser_rejects_duplicate_sort_fields() {
    let error = parse_query_parameter("sort=id.asc,-id").unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest("duplicate sort field 'id'".to_string())
    );
}

#[test]
fn parses_revision_filters_and_rejects_invalid_positive_bigints() {
    let parsed = parse_query_parameter(
        "revision__between=10,20&before_revision__gte=3&after_revision__in=4,5&sort=-revision",
    )
    .unwrap();

    assert_eq!(parsed.filters[0].field, FilterField::Revision);
    assert_eq!(parsed.filters[1].field, FilterField::BeforeRevision);
    assert_eq!(parsed.filters[2].field, FilterField::AfterRevision);
    assert_eq!(parsed.sort[0].field, FilterField::Revision);
    assert!(parsed.sort[0].descending);
    assert_eq!(
        parse_positive_bigint_list_with_limit(&i64::MAX.to_string(), 1).unwrap(),
        vec![i64::MAX]
    );
    for invalid in ["0", "-1", "9223372036854775808", "x", "1,"] {
        assert!(parse_positive_bigint_list_with_limit(invalid, 8).is_err());
    }
    assert!(parse_positive_bigint_list_with_limit("1,2,3", 2).is_err());
}

#[test]
fn parses_shared_and_personal_computed_queries() {
    let parsed = parse_query_parameter(
        "sort=computed.shared.display_name.asc,computed.personal.my_rank.desc",
    )
    .unwrap();

    let shared = parsed.sort[0].field.computed_query().unwrap();
    assert_eq!(shared.scope(), ComputedFieldScope::Shared);
    assert_eq!(shared.key(), "display_name");
    assert!(!parsed.sort[0].descending);
    let personal = parsed.sort[1].field.computed_query().unwrap();
    assert_eq!(personal.scope(), ComputedFieldScope::Personal);
    assert_eq!(personal.key(), "my_rank");
    assert!(parsed.sort[1].descending);
}

#[test]
fn computed_query_preserves_a_key_named_asc() {
    let parsed = parse_query_parameter("sort=computed.shared.asc").unwrap();

    let computed = parsed.sort[0].field.computed_query().unwrap();
    assert_eq!(computed.key(), "asc");
    assert!(!parsed.sort[0].descending);
}

#[test]
fn computed_query_preserves_a_key_named_desc() {
    let parsed = parse_query_parameter("sort=computed.shared.desc").unwrap();

    let computed = parsed.sort[0].field.computed_query().unwrap();
    assert_eq!(computed.key(), "desc");
    assert!(!parsed.sort[0].descending);
}

#[test]
fn computed_query_direction_can_be_set_for_direction_named_keys() {
    let parsed = parse_query_parameter(
        "sort=computed.shared.asc.desc,-computed.shared.desc,computed.personal.desc.asc",
    )
    .unwrap();

    assert_eq!(parsed.sort[0].field.computed_query().unwrap().key(), "asc");
    assert!(parsed.sort[0].descending);
    assert_eq!(parsed.sort[1].field.computed_query().unwrap().key(), "desc");
    assert!(parsed.sort[1].descending);
    assert_eq!(parsed.sort[2].field.computed_query().unwrap().key(), "desc");
    assert!(!parsed.sort[2].descending);
}

#[test]
fn accepts_public_and_private_computed_query_aliases() {
    let parsed =
        parse_query_parameter("sort=computed.public.display_name,computed.private.my_rank.desc")
            .unwrap();

    assert_eq!(
        parsed.sort[0].field.computed_query().unwrap().scope(),
        ComputedFieldScope::Shared
    );
    assert_eq!(
        parsed.sort[1].field.computed_query().unwrap().scope(),
        ComputedFieldScope::Personal
    );
}

#[test]
fn rejects_computed_filters_without_resource_opt_in() {
    let error = parse_query_parameter("computed.shared.display_name=router").unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest(
            "Computed fields are not supported in this filter context".to_string()
        )
    );
}

#[test]
fn resource_opt_in_accepts_computed_filters_and_aliases() {
    let (parsed, _) = parse_query_parameter_with_computed_filters_and_passthrough(
        "computed.public.display_name__icontains=edge&computed.private.rank__gte=2",
        &[],
    )
    .unwrap();

    let shared = parsed.filters[0].field.computed_query().unwrap();
    assert_eq!(shared.scope(), ComputedFieldScope::Shared);
    assert_eq!(shared.key(), "display_name");
    assert_eq!(
        parsed.filters[0].operator,
        SearchOperator::IContains { is_negated: false }
    );
    let personal = parsed.filters[1].field.computed_query().unwrap();
    assert_eq!(personal.scope(), ComputedFieldScope::Personal);
    assert_eq!(personal.key(), "rank");
    assert_eq!(
        parsed.filters[1].operator,
        SearchOperator::Gte { is_negated: false }
    );
}

#[test]
fn computed_filter_preserves_double_underscores_in_the_key() {
    let (parsed, _) = parse_query_parameter_with_computed_filters_and_passthrough(
        "computed.shared.display__name=router",
        &[],
    )
    .unwrap();

    let computed = parsed.filters[0].field.computed_query().unwrap();
    assert_eq!(computed.key(), "display__name");
    assert_eq!(
        parsed.filters[0].operator,
        SearchOperator::Equals { is_negated: false }
    );
}

#[test]
fn computed_filter_recognizes_only_a_terminal_operator_suffix() {
    let (parsed, _) = parse_query_parameter_with_computed_filters_and_passthrough(
        "computed.shared.display__name__icontains=edge",
        &[],
    )
    .unwrap();

    let computed = parsed.filters[0].field.computed_query().unwrap();
    assert_eq!(computed.key(), "display__name");
    assert_eq!(
        parsed.filters[0].operator,
        SearchOperator::IContains { is_negated: false }
    );
}

#[test]
fn ordinary_filter_still_rejects_an_unknown_operator() {
    let error = parse_query_parameter("name__unknown=router").unwrap_err();

    assert_eq!(error.to_string(), "Invalid search operator: 'unknown'");
}

#[test]
fn rejects_invalid_computed_query_keys() {
    let error = parse_query_parameter("sort=computed.shared.Invalid-Key").unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid computed field key: 'Invalid-Key'"
    );
}

#[test]
fn include_total_defaults_to_true() {
    let parsed = parse_query_parameter("").unwrap();

    assert!(parsed.include_total);
}

#[test]
fn include_total_accepts_false() {
    let parsed = parse_query_parameter("include_total=false").unwrap();

    assert!(!parsed.include_total);
}

#[test]
fn include_total_rejects_duplicates() {
    let error = parse_query_parameter("include_total=true&include_total=false").unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest("duplicate include_total".to_string())
    );
}

#[test]
fn include_total_rejects_invalid_boolean() {
    let error = parse_query_parameter("include_total=yes").unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest("Invalid boolean value: 'yes'".to_string())
    );
}

#[test]
fn passthrough_preserves_repeated_values() {
    let (parsed, passthrough) =
        parse_query_parameter_with_passthrough("name=router&local=one&local=two", &["local"])
            .unwrap();

    assert_eq!(parsed.filters.len(), 1);
    assert_eq!(passthrough["local"], ["one", "two"]);
}

#[test]
fn decodes_form_encoded_filter_values() {
    let parsed = parse_query_parameter("name=core+router%2Fedge").unwrap();

    assert_eq!(parsed.filters[0].value(), "core router/edge");
}

#[test]
fn shared_query_decoder_decodes_keys_and_values() {
    let pairs = decode_query_parameter_pairs("search+term=core+router%2Fedge").unwrap();

    assert_eq!(
        pairs,
        [("search term".to_string(), "core router/edge".to_string())]
    );
}

#[test]
fn shared_query_decoder_accepts_an_empty_query_string() {
    assert_eq!(decode_query_parameter_pairs("").unwrap(), []);
}

#[test]
fn shared_query_decoder_borrows_unchanged_components() {
    let (key, value) = decode_query_parameter_pair("kind=object").unwrap();

    assert!(matches!(key, Cow::Borrowed("kind")));
    assert!(matches!(value, Cow::Borrowed("object")));
}

#[test]
fn parses_integer_ranges() {
    assert_eq!(
        parse_integer_list("1-4,3,8,-4--2").unwrap(),
        vec![-4, -3, -2, 1, 2, 3, 4, 8]
    );
}

#[test]
fn rejects_integer_ranges_above_the_expansion_limit() {
    let error = parse_integer_list("0-2147483647").unwrap_err();

    assert_eq!(
        error,
        QueryError::InvalidIntegerRange(format!(
            "Integer filter '0-2147483647' expands to more than {MAX_INTEGER_FILTER_VALUES} unique values"
        ))
    );
}

#[test]
fn integer_list_limit_counts_unique_values() {
    assert_eq!(
        parse_integer_list_with_limit("1-3,1-3,2", 3).unwrap(),
        vec![1, 2, 3]
    );
}

#[test]
fn rejects_an_integer_list_when_the_unique_value_limit_is_zero() {
    let error = parse_integer_list_with_limit("1", 0).unwrap_err();

    assert_eq!(
        error,
        QueryError::InvalidIntegerRange(
            "Integer filter '1' expands to more than 0 unique values".to_string()
        )
    );
}

#[test]
fn json_path_preserves_valid_nested_segments() {
    let path = JsonFieldPath::new("network,address_v4,$value").unwrap();

    assert_eq!(
        path.segments().collect::<Vec<_>>(),
        ["network", "address_v4", "$value"]
    );
    assert_eq!(path.canonical(), "network,address_v4,$value");
    assert_eq!(path.to_string(), "network,address_v4,$value");
}

#[test]
fn json_path_rejects_empty_segments() {
    let error = JsonFieldPath::new("network,,address").unwrap_err();

    assert_eq!(
        error,
        QueryError::BadRequest(
            "Invalid JSON path 'network,,address'; use non-empty comma-separated ASCII path segments containing only letters, digits, '_', or '$'"
                .to_string()
        )
    );
}

#[test]
fn infers_scalar_type_from_value_and_operator() {
    assert_eq!(
        infer_query_scalar_type("2024-01-15", Operator::Equals),
        Some(QueryScalarType::Date)
    );
    assert_eq!(
        infer_query_scalar_type("router", Operator::IContains),
        Some(QueryScalarType::String)
    );
}

#[test]
fn storage_continuation_does_not_relax_the_public_cursor_limit() {
    let sorts = vec![SortParam::new(FilterField::Description, false)];
    let values = vec![CursorValue::String("x".repeat(MAX_ENCODED_CURSOR_BYTES))];
    let continuation = QueryContinuation::new(&sorts, values.clone()).unwrap();
    let mut query = QueryOptions::new(vec![], sorts.clone(), Some(1), None, false).unwrap();
    query.set_continuation(continuation);
    assert_eq!(
        query.cursor_values(&sorts).unwrap().unwrap().as_ref(),
        &values
    );
    assert!(encode_cursor_values(&sorts, values).is_err());
    assert!(
        query
            .set_cursor(Some("x".repeat(MAX_ENCODED_CURSOR_BYTES + 1)))
            .is_err()
    );
    assert!(
        query.continuation().is_some(),
        "a rejected token must preserve the continuation"
    );
}

#[rstest::rstest]
#[case::clear(None)]
#[case::replace(Some("client-token"))]
fn public_cursor_update_clears_storage_continuation(#[case] cursor: Option<&str>) {
    let sorts = [SortParam::new(FilterField::Id, false)];
    let mut query = QueryOptions::empty();
    query.set_continuation(QueryContinuation::new(&sorts, vec![CursorValue::Integer(1)]).unwrap());
    if let Some(cursor) = cursor {
        query.set_validated_cursor(Some(QueryCursor::new(cursor.to_string()).unwrap()));
    } else {
        query.clear_cursor();
    }
    assert!(query.continuation().is_none());
    assert_eq!(query.cursor().map(QueryCursor::as_str), cursor);
    assert_eq!(query.has_cursor(), cursor.is_some());
}

#[test]
fn storage_continuation_rejects_a_different_sort_order() {
    let mut query = QueryOptions::empty();
    query.set_continuation(
        QueryContinuation::new(
            &[SortParam::new(FilterField::Id, false)],
            vec![CursorValue::Integer(1)],
        )
        .unwrap(),
    );
    assert!(
        query
            .cursor_values(&[SortParam::new(FilterField::Id, true)])
            .is_err()
    );
}

#[rstest::rstest]
#[case::wrong_count(vec![])]
#[case::nul(vec![CursorValue::String("a\0b".to_string())])]
#[case::invalid_decimal(vec![CursorValue::Decimal("1); SELECT 1".to_string())])]
fn storage_continuation_validates_values_before_the_adapter(#[case] values: Vec<CursorValue>) {
    assert!(QueryContinuation::new(&[SortParam::new(FilterField::Id, false)], values).is_err());
}

#[test]
fn storage_continuation_normalizes_decimal_values_like_public_tokens() {
    let sorts = [SortParam::new(FilterField::Id, false)];
    let values = vec![CursorValue::Decimal("1.00e2".to_string())];
    let encoded = encode_cursor_values(&sorts, values.clone()).unwrap();
    let mut query = QueryOptions::empty();
    query.set_continuation(QueryContinuation::new(&sorts, values).unwrap());
    assert_eq!(
        query.cursor_values(&sorts).unwrap().unwrap().as_ref(),
        decode_cursor_values(&encoded, &sorts).unwrap()
    );
}

#[test]
fn cursor_codec_round_trips_values_for_the_exact_sort_order() {
    let sorts = [SortParam {
        field: FilterField::Id,
        descending: false,
    }];
    let cursor = encode_cursor_values(&sorts, vec![CursorValue::Integer(42)]).unwrap();

    assert_eq!(
        decode_cursor_values(&cursor, &sorts).unwrap(),
        [CursorValue::Integer(42)]
    );
}

#[test]
fn cursor_codec_rejects_a_different_sort_order() {
    let ascending = [SortParam {
        field: FilterField::Id,
        descending: false,
    }];
    let descending = [SortParam {
        field: FilterField::Id,
        descending: true,
    }];
    let cursor = encode_cursor_values(&ascending, vec![CursorValue::Integer(42)]).unwrap();

    let error = decode_cursor_values(&cursor, &descending).unwrap_err();

    assert_eq!(
        error,
        CursorCodecError::Invalid("cursor does not match current sort order".to_string())
    );
}

#[test]
fn cursor_codec_canonicalizes_bounded_decimals_on_decode() {
    let sorts = [SortParam {
        field: FilterField::Id,
        descending: false,
    }];
    let cursor =
        encode_cursor_values(&sorts, vec![CursorValue::Decimal("1.00".to_string())]).unwrap();

    assert_eq!(
        decode_cursor_values(&cursor, &sorts).unwrap(),
        [CursorValue::Decimal("1".to_string())]
    );
}

#[rstest]
#[case("true", true)]
#[case("TRUE", true)]
#[case("TrUe", true)]
#[case("false", false)]
#[case("FALSE", false)]
#[case("FaLsE", false)]
fn boolean_parser_accepts_ascii_case_variants(#[case] input: &str, #[case] expected: bool) {
    assert_eq!(parse_boolean_value(input).unwrap(), expected);
}

#[rstest]
#[case("")]
#[case(" true")]
#[case("false ")]
#[case("1")]
#[case("yes")]
#[case("falſe")]
#[case("ＴＲＵＥ")]
fn boolean_parser_preserves_invalid_input_in_errors(#[case] input: &str) {
    assert_eq!(
        parse_boolean_value(input).unwrap_err(),
        QueryError::BadRequest(format!("Invalid boolean value: '{input}'"))
    );
}

#[test]
fn shared_scalar_parser_accepts_timestamp_forms() {
    assert_eq!(
        parse_datetime_list("2026-08-14,2026-08-14T12:30:00+02:00")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn shared_scalar_parsers_reject_invalid_values() {
    assert!(parse_boolean_value("sometimes").is_err());
    assert!(parse_datetime_list("14/08/2026").is_err());
}
