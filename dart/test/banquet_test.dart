import 'package:test/test.dart';
import 'package:banquet/banquet.dart';

void main() {
  // ---------------------------------------------------------------------------
  // TestCleanUrl
  // ---------------------------------------------------------------------------
  test('TestCleanUrl', () {
    final url =
        '/http:/darianhickman.com:8080/some/local/path/file.csv;col1,col2,col3';
    final cleaned = cleanUrl(url);
    expect(
      cleaned,
      equals(
          'http://darianhickman.com:8080/some/local/path/file.csv;col1,col2,col3'),
    );
  });

  // ---------------------------------------------------------------------------
  // TestLocalParse
  // ---------------------------------------------------------------------------
  test('TestLocalParse - simple CSV', () {
    final b = parseNested(
        'http://localhost:8080/some/local/path/file.csv;col1,col2,col3');

    expect(b.dataSetPath, equals('some/local/path/file.csv'),
        reason: 'DataSetPath');
    expect(b.table, equals('col1,col2,col3'), reason: 'Table');
    expect(b.select, equals(['*']), reason: 'Select');
  });

  test('TestLocalParse - complex gs:// URL', () {
    final complexURL =
        'http://localhost:8080/gs:/my-bucket/database.sqlite;customers;id,name,+age?where=age>18&limit=50';
    final bc = parseNested(complexURL);

    expect(bc.scheme, equals('gs'), reason: 'Scheme');
    expect(bc.host, equals('my-bucket'), reason: 'Host');
    expect(bc.dataSetPath, equals('/database.sqlite'), reason: 'DataSetPath');
    expect(bc.table, equals('customers'), reason: 'Table');
    expect(bc.where, equals('age>18'), reason: 'Where');
    expect(bc.orderBy, equals('age'), reason: 'OrderBy');
    expect(bc.limit, equals('50'), reason: 'Limit');
    expect(bc.select, equals(['id', 'name']), reason: 'Select');
  });

  // ---------------------------------------------------------------------------
  // TestParseNested
  // ---------------------------------------------------------------------------
  test('TestParseNested', () {
    final reqURL =
        'https://localhost:8080/gs:/matrix@bucket.appspot.com:8080/some/file/path.csv/column1,column2/+column3?orderid=1';
    final b = parseNested(reqURL);

    expect(b.scheme, equals('gs'), reason: 'Scheme');
    expect(b.username(), equals('matrix'), reason: 'Username');
    expect(b.host, equals('bucket.appspot.com:8080'), reason: 'Host');
    expect(b.port(), equals('8080'), reason: 'Port');
    expect(b.path,
        equals('/some/file/path.csv/column1,column2/+column3'),
        reason: 'Path');
    expect(b.rawQuery, equals('orderid=1'), reason: 'RawQuery');
  });

  // ---------------------------------------------------------------------------
  // TestParseBanquet
  // ---------------------------------------------------------------------------
  test('TestParseBanquet', () {
    final testURL =
        'gs://bucket.appspot.com:8080/some/file/path.csv/column1,column2,+column3?where=age>20&limit=10&offset=5&groupby=department&having=count>1';
    final b = parseBanquet(testURL);

    expect(b.scheme, equals('gs'), reason: 'Scheme');
    expect(b.host, equals('bucket.appspot.com:8080'), reason: 'Host');
    expect(b.table, equals(''), reason: 'Table (heuristic path, should be empty)');
    expect(b.select, equals(['column1', 'column2']), reason: 'Select');
    expect(b.orderBy, equals('column3'), reason: 'OrderBy');
    expect(b.where, equals('age>20'), reason: 'Where');
    expect(b.limit, equals('10'), reason: 'Limit');
    expect(b.offset, equals('5'), reason: 'Offset');
    expect(b.groupBy, equals('department'), reason: 'GroupBy');
    expect(b.having, equals('count>1'), reason: 'Having');
  });

  // ---------------------------------------------------------------------------
  // TestParseNestedUrl
  // ---------------------------------------------------------------------------
  test('TestParseNestedUrl', () {
    final reqURL =
        'https://localhost:8080/gs:/matrix@bucket.appspot.com:8080/some/file/path.csv/column1,column2,+column3?orderid=1';
    final b = parseNested(reqURL);

    expect(b.scheme, equals('gs'), reason: 'Scheme');
    expect(b.host, equals('bucket.appspot.com:8080'), reason: 'Host');
    expect(b.port(), equals('8080'), reason: 'Port');
    expect(b.path,
        equals('/some/file/path.csv/column1,column2,+column3'),
        reason: 'Path');
    expect(b.rawQuery, equals('orderid=1'), reason: 'RawQuery');
  });

  // ---------------------------------------------------------------------------
  // TestParseSelect
  // ---------------------------------------------------------------------------
  test('TestParseSelect', () {
    final afterTable = 'column1,+column2,-column3';
    // Sort prefixes exclude a column from the SELECT list
    final result = parseSelect(afterTable);
    expect(result, equals(['column1']));
  });

  // ---------------------------------------------------------------------------
  // TestParseGroupBy
  // ---------------------------------------------------------------------------
  test('TestParseGroupBy', () {
    final afterPart = 'some_column(group_column)';
    final result = parseGroupBy(afterPart, '');
    expect(result, equals('group_column'));
  });

  // ---------------------------------------------------------------------------
  // TestParseLegacyLiteral (^ and !^ treated as literal column names)
  // ---------------------------------------------------------------------------
  test('TestParseLegacyLiteral', () {
    final afterTable = '^column1,!^column2';
    // Neither ^ nor !^ are kAsc (+) or kDesc (-), so they should be treated
    // as literal column names and appear in the select list.
    final expected = ['^column1', '!^column2'];
    final result = parseSelect(afterTable);
    expect(result, equals(expected));

    // Also verify no orderBy is derived from these literals
    // (we access the private helper via the public parseBanquet result)
    final b = parseBanquet('file://some.csv/$afterTable');
    expect(b.orderBy, equals(''), reason: 'No orderBy for legacy literals');
    expect(b.sortDirection, equals(''),
        reason: 'No sortDirection for legacy literals');
  });
}
