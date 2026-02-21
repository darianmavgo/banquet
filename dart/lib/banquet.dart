/// Package banquet provides a framework for standardizing and parsing URLs for
/// tabular data access. It supports querying datasets (CSV, SQLite, etc.) via
/// URL paths and query parameters, allowing typical SQL-like operations such as
/// selection, filtering, sorting, limiting, and grouping directly from the URL.
///
/// Banquet Classifications & URL Structure:
///
/// 1. Flat (1-tier):          path/to/dataset;;Column
/// 2. Nested Table (2-tier):  path/to/dataset;Table
/// 3. Nested Column (3-tier): path/to/dataset;Table;Column
///
/// Fallback Convention (semicolon-less):
/// If no semicolons are present, the package heuristically determines the table
/// or column based on file extensions.
///
/// Supported Prefixes/Suffixes:
/// - Sort: +column (ASC), -column (DESC)
/// - Slice: [start:end] (translated to LIMIT/OFFSET)
library banquet;

bool _verbose = false;

/// Sets verbose logging for the package.
void setVerbose(bool v) => _verbose = v;

/// Returns true if verbose logging is enabled.
bool isVerbose() => _verbose;

/// ASC is the prefix token to signal ascending sort order.
const String kAsc = '+';

/// DESC is the prefix token to signal descending sort order.
const String kDesc = '-';

// ---------------------------------------------------------------------------
// Banquet
// ---------------------------------------------------------------------------

/// Banquet represents a parsed URL request for tabular data.
/// It wraps [Uri] with SQL-like clauses derived from the path and query params.
class Banquet {
  /// The underlying parsed URI (replaces Go's *url.URL).
  final Uri uri;

  String where = '';
  String table = '';
  List<String> select = const ['*'];
  String sortDirection = '';
  String limit = '';
  String offset = '';
  String groupBy = '';
  String having = '';
  String orderBy = '';

  /// Path to the source dataset file (e.g., .csv, .sqlite).
  String dataSetPath = '';

  /// The remaining path segment after the dataset, containing columns, sort
  /// instructions, or conditions.
  String columnPath = '';

  // Internal fields
  final String _rawurl;

  Banquet._({
    required this.uri,
    required String rawurl,
    this.dataSetPath = '',
    this.table = '',
    this.columnPath = '',
  }) : _rawurl = rawurl;

  // Convenience passthrough getters to mirror Go's embedded *url.URL fields.
  String get scheme => uri.scheme;
  /// Returns the host exactly as Go's url.URL.Host does – including port when present.
  /// e.g. 'bucket.appspot.com:8080' not just 'bucket.appspot.com'.
  String get host => uri.hasPort ? '${uri.host}:${uri.port}' : uri.host;
  String get path => uri.path;
  String get rawQuery => uri.query;
  String port() => uri.hasPort ? '${uri.port}' : '';


  /// Returns the userinfo username (mirrors Go's b.User.Username()).
  String username() => uri.userInfo.contains(':')
      ? uri.userInfo.split(':').first
      : uri.userInfo;

  @override
  String toString() =>
      'rawurl: $_rawurl\n'
      '  S: $scheme H: $host DP: $dataSetPath CP: $columnPath RQ: $rawQuery TB: "$table"';
}

// ---------------------------------------------------------------------------
// CleanUrl
// ---------------------------------------------------------------------------

/// CleanUrl prepares a raw URL string for standard parsing.
/// Mirrors [banquet.CleanUrl] in Go exactly.
String cleanUrl(String rawurl) {
  if (rawurl == '/') return '.';
  rawurl = rawurl.replaceFirst(RegExp(r'^/'), '');

  final schemeIdx = rawurl.indexOf(':/');
  if (schemeIdx != -1) {
    // Ensure :/ becomes :// (e.g. gs:/ -> gs://)
    if (!rawurl.substring(schemeIdx).startsWith('://')) {
      rawurl = rawurl.replaceFirst(':/', '://');
    }
  } else {
    // No scheme separator present – guard against colon-in-first-segment issue.
    if (!rawurl.contains('://') && rawurl.contains(':')) {
      final slashIdx = rawurl.indexOf('/');
      final colonIdx = rawurl.indexOf(':');
      if (slashIdx == -1 || colonIdx < slashIdx) {
        return './$rawurl';
      }
    }
  }
  return rawurl;
}

// ---------------------------------------------------------------------------
// ParseBanquet
// ---------------------------------------------------------------------------

/// Parses a raw URL string into a [Banquet] object.
/// Mirrors [banquet.ParseBanquet] in Go exactly.
Banquet parseBanquet(String rawurl) {
  if (_verbose) print('[BANQUET] Parsing URL: $rawurl');

  rawurl = cleanUrl(rawurl);
  if (_verbose) print('[BANQUET] Cleaned URL: $rawurl');

  final Uri u;
  try {
    u = Uri.parse(rawurl);
  } catch (e) {
    if (_verbose) print('[BANQUET] URL parse error: $e');
    rethrow;
  }

  final b = Banquet._(uri: u, rawurl: rawurl);

  final (dsPath, tbl, colPath) = _parseDataSetColumnPath(u.path);
  b.dataSetPath = dsPath;
  b.table = tbl;
  b.columnPath = colPath;

  if (_verbose) {
    print('[BANQUET] DataSetPath: ${b.dataSetPath}, Table: "${b.table}", ColumnPath: ${b.columnPath}');
  }

  // Fallback heuristic table identification
  if (b.table.isEmpty) {
    b.table = _parseTable(b.columnPath);
    if (_verbose) print('[BANQUET] Table identified via heuristic: ${b.table}');
  }

  // Strip slice notation from table
  final bracketIdx = b.table.indexOf('[');
  if (bracketIdx != -1) {
    b.table = b.table.substring(0, bracketIdx);
  }

  b.select = parseSelect(b.columnPath);
  if (_verbose) print('[BANQUET] Selected columns: ${b.select}');

  // If Select is just the table name, default to *
  if (b.select.length == 1 && b.select[0] == b.table) {
    b.select = ['*'];
  }

  final queryWhere = _parseWhere(u.query);
  final pathWhere = _parsePathConditions(b.columnPath);

  if (pathWhere.isNotEmpty) {
    b.where =
        queryWhere.isNotEmpty ? '$queryWhere AND $pathWhere' : pathWhere;
  } else {
    b.where = queryWhere;
  }

  if (_verbose && b.where.isNotEmpty) {
    print('[BANQUET] effective WHERE: ${b.where}');
  }

  b.groupBy = parseGroupBy(u.path, u.query);
  b.limit = _parseLimit(u.query, u.path);
  b.offset = _parseOffset(u.query, u.path);
  b.having = _parseHaving(u.query);

  final (ob, dir) = _parseOrderBy(b.columnPath, u.query);
  if (ob.isNotEmpty) {
    b.orderBy = ob;
    if (dir.isNotEmpty) b.sortDirection = dir;
  }

  return b;
}

// ---------------------------------------------------------------------------
// ParseNested
// ---------------------------------------------------------------------------

/// Extracts and parses a Banquet URL that wraps an inner URL.
/// Mirrors [banquet.ParseNested] in Go exactly.
Banquet parseNested(String rawURL) {
  if (rawURL != '/') {
    rawURL = rawURL.replaceFirst(RegExp(r'^/'), '');
  }

  final Uri outer;
  try {
    outer = Uri.parse(rawURL);
  } catch (e) {
    rethrow;
  }

  // Use the raw (encoded) path, equivalent to outer.EscapedPath() in Go.
  String inner = outer.path;

  if (outer.query.isNotEmpty) {
    inner = '$inner?${outer.query}';
  }

  try {
    return parseBanquet(inner);
  } catch (e) {
    print("Error parsing inner URL '$inner': $e. Continuing with raw URL.");
    // Best-effort fallback (mirrors Go's partial Banquet return).
    return Banquet._(uri: Uri(path: inner), rawurl: inner);
  }
}

// ---------------------------------------------------------------------------
// ParseSelect (exported)
// ---------------------------------------------------------------------------

/// Parses the column path to determine which columns to select.
/// Mirrors [banquet.ParseSelect] in Go exactly.
List<String> parseSelect(String columnPath) {
  final segments = _getSegments(columnPath);
  if (segments.isEmpty) return ['*'];

  final collected = <String>[];
  for (final segment in segments) {
    if (segment.isEmpty) continue;

    final cols = segment.split(',');
    for (var col in cols) {
      // Strip slice notation only if it contains ':'
      final idx = col.indexOf('[');
      if (idx != -1 && col.substring(idx).contains(':')) {
        col = col.substring(0, idx);
      }

      col = col.trim();
      if (col.isEmpty) continue;

      // Conditions are not selections
      if (col.contains('!=')) continue;

      // Sort prefixes mean this col is for ordering, not selection
      if (col.startsWith(kAsc) || col.startsWith(kDesc)) continue;

      collected.add(col);
    }
  }

  return collected.isEmpty ? ['*'] : collected;
}

// ---------------------------------------------------------------------------
// ParseGroupBy (exported)
// ---------------------------------------------------------------------------

/// Parses the group-by clause from the path or query.
/// Mirrors [banquet.ParseGroupBy] in Go exactly.
String parseGroupBy(String path, String query) {
  final params = Uri.splitQueryString(query);
  final g = params['groupby'] ?? '';
  if (g.isNotEmpty) return g;

  // Check path for (expression)
  if (path.contains('(') && path.contains(')')) {
    final start = path.indexOf('(');
    final end = path.indexOf(')');
    if (start < end) return path.substring(start + 1, end);
  }
  return '';
}

// ---------------------------------------------------------------------------
// FmtPrintln / FmtSprintf helpers
// ---------------------------------------------------------------------------

void fmtPrintln(Banquet b) {
  print('rawurl: ${b._rawurl}\n'
      'Scheme: ${b.scheme}\n'
      '  Host:   ${b.host}\n'
      '  DataSetPath: ${b.dataSetPath}\n'
      '  ColumnPath: ${b.columnPath}\n'
      '  RawQuery: ${b.rawQuery}\n'
      '  Table:      "${b.table}"');
}

String fmtSprintf(Banquet b) =>
    'rawurl: ${b._rawurl}\\n'
    '  S: ${b.scheme} H: ${b.host} DP: ${b.dataSetPath}'
    'CP: ${b.columnPath}RQ:${b.rawQuery}TB:"${b.table}"';

// ---------------------------------------------------------------------------
// Private helpers (mirrors unexported Go functions)
// ---------------------------------------------------------------------------

/// Mirrors [banquet.parseDataSetColumnPath].
(String datasetPath, String table, String columnPath) _parseDataSetColumnPath(
    String rawpath) {
  if (rawpath.contains(';')) {
    final parts = rawpath.split(';');
    final datasetPath = parts[0];
    final table = parts.length > 1 ? parts[1] : '';
    final columnPath = parts.length > 2 ? parts.sublist(2).join(';') : '';
    return (datasetPath, table, columnPath);
  }

  // Extension-based splitting (no semicolons)
  final parts = rawpath.split('/');
  const knownExts = ['.zip', '.csv', '.sqlite', '.db', '.xlsx', '.json', '.txt'];

  for (var i = 0; i < parts.length; i++) {
    final part = parts[i];
    final isHtml =
        part.endsWith('.html') && part != 'test.html';
    final hasExt = knownExts.any((e) => part.endsWith(e)) || isHtml;
    if (hasExt) {
      final datasetPath = parts.sublist(0, i + 1).join('/');
      final columnPath =
          i + 1 < parts.length ? parts.sublist(i + 1).join('/') : '';
      return (datasetPath, '', columnPath);
    }
  }
  return (rawpath, '', '');
}

/// Mirrors [banquet.getSegments].
List<String> _getSegments(String columnPath) {
  final parts = columnPath.split('/');
  if (parts.isEmpty || (parts.length == 1 && parts[0].isEmpty)) return [];

  var firstClear = -1;
  for (var i = 0; i < parts.length; i++) {
    final part = parts[i];
    if (part.contains(',') ||
        part.startsWith(kAsc) ||
        part.startsWith(kDesc) ||
        part.contains('!=') ||
        (part.startsWith('[') && part.contains(':'))) {
      firstClear = i;
      break;
    }
  }

  if (firstClear != -1) return parts.sublist(firstClear);
  return [parts.last];
}

/// Mirrors [banquet.parsePathConditions].
String _parsePathConditions(String columnPath) {
  final segments = _getSegments(columnPath);
  if (segments.isEmpty) return '';

  final conditions = <String>[];
  for (final segment in segments) {
    if (segment.isEmpty) continue;
    final parts = segment.split(',');
    for (final part in parts) {
      if (part.contains('!=')) {
        final kv = part.split('!=');
        if (kv.length == 2) {
          final col = kv[0].trim();
          var val = kv[1].trim();

          // URL-decode the value (tolerant)
          try {
            val = Uri.decodeQueryComponent(val);
          } catch (_) {}

          // Quote if not a number
          if (double.tryParse(val) == null) {
            val = val.replaceAll("'", "''");
            val = "'$val'";
          }
          conditions.add('$col != $val');
        }
      }
    }
  }
  return conditions.isEmpty ? '' : conditions.join(' AND ');
}

/// Mirrors [banquet.parseWhere].
String _parseWhere(String query) {
  if (query.isEmpty) return '';
  // Tolerant split – same approach as Go (manual split, not Uri.splitQueryString).
  final params = query.split('&');
  for (final p in params) {
    if (p.startsWith('where=')) {
      final val = p.substring('where='.length);
      try {
        return Uri.decodeQueryComponent(val);
      } catch (_) {
        return val;
      }
    }
  }
  return '';
}

/// Mirrors [banquet.parseTable].
String _parseTable(String columnPath) {
  if (columnPath.isEmpty) return '';

  final parts = columnPath
      .replaceAll(RegExp(r'^/+|/+$'), '')
      .split('/');
  if (parts.isEmpty || parts[0].isEmpty) return '';

  final first = parts[0];

  if (first.contains(',') ||
      first.startsWith(kAsc) ||
      first.startsWith(kDesc) ||
      first.contains('!=') ||
      first.contains('=') ||
      first.contains('>') ||
      first.contains('<') ||
      (first.startsWith('[') && first.contains(':'))) {
    return '';
  }
  return first;
}

/// Mirrors [banquet.parseLimit].
String _parseLimit(String query, String path) {
  final params = Uri.splitQueryString(query);
  final l = params['limit'] ?? '';
  if (l.isNotEmpty) return l;
  final (limit, _) = _parseSlice(path);
  return limit;
}

/// Mirrors [banquet.parseOffset].
String _parseOffset(String query, String path) {
  final params = Uri.splitQueryString(query);
  final o = params['offset'] ?? '';
  if (o.isNotEmpty) return o;
  final (_, offset) = _parseSlice(path);
  return offset;
}

/// Mirrors [banquet.parseHaving].
String _parseHaving(String query) {
  final params = Uri.splitQueryString(query);
  return params['having'] ?? '';
}

/// Mirrors [banquet.parseOrderBy].
(String orderBy, String direction) _parseOrderBy(
    String columnPath, String query) {
  final params = Uri.splitQueryString(query);
  final ob = params['orderby'] ?? '';
  if (ob.isNotEmpty) return (ob, '');

  final parts = columnPath.split('/');
  for (final part in parts) {
    final cols = part.split(',');
    for (var col in cols) {
      col = col.trim();
      final bracketIdx = col.indexOf('[');
      if (bracketIdx != -1) col = col.substring(0, bracketIdx);

      if (col.startsWith(kAsc)) {
        return (col.substring(kAsc.length), 'ASC');
      }
      if (col.startsWith(kDesc)) {
        return (col.substring(kDesc.length), 'DESC');
      }
    }
  }
  return ('', '');
}

/// Mirrors [banquet.parseSlice].
/// Returns (limit, offset) strings, matching Go's (string, string) return.
(String limit, String offset) _parseSlice(String pathStr) {
  final startIdx = pathStr.lastIndexOf('[');
  if (startIdx == -1) return ('', '');

  final sub = pathStr.substring(startIdx);
  final endIdx = sub.indexOf(']');
  if (endIdx == -1) return ('', '');

  final content = sub.substring(1, endIdx);
  final parts = content.split(':');
  if (parts.length != 2) return ('', '');

  final startStr = parts[0].trim();
  final endStr = parts[1].trim();

  final int start;
  final int? end;

  if (startStr.isEmpty) {
    start = 0;
  } else {
    start = int.tryParse(startStr) ?? -1;
    if (start == -1) return ('', '');
  }

  if (endStr.isEmpty) {
    end = null;
  } else {
    end = int.tryParse(endStr);
    if (end == null) return ('', '');
  }

  final int? l = end != null ? (end - start < 0 ? 0 : end - start) : null;
  final limitStr = l != null ? '$l' : '';
  final offsetStr = '$start';

  return (limitStr, offsetStr);
}
