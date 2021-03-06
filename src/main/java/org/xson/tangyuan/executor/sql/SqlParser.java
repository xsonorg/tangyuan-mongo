package org.xson.tangyuan.executor.sql;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.xson.tangyuan.executor.sql.ValueVo.ValueType;
import org.xson.tangyuan.executor.sql.condition.BracketsCondition;
import org.xson.tangyuan.executor.sql.condition.EqualCondition;
import org.xson.tangyuan.executor.sql.condition.GreaterEqualCondition;
import org.xson.tangyuan.executor.sql.condition.InCondition;
import org.xson.tangyuan.executor.sql.condition.LessThanCondition;
import org.xson.tangyuan.executor.sql.condition.LessThanEqualCondition;
import org.xson.tangyuan.executor.sql.condition.LikeCondition;
import org.xson.tangyuan.executor.sql.condition.MoreThanCondition;
import org.xson.tangyuan.executor.sql.condition.NotEqualCondition;
import org.xson.tangyuan.executor.sql.condition.NotInCondition;

public class SqlParser {

	// public final static int START = 7;
	// Mark

	public final static String	BLANK_MARK		= " ";
	public final static String	SELECT_MARK		= "SELECT";
	public final static String	INSERT_MARK		= "INSERT INTO";
	public final static String	UPDATE_MARK		= "UPDATE";
	public final static String	DELETE_MARK		= "DELETE FROM";

	// public final static String FROM_MARK = " FROM ";
	// public final static String WHERE_MARK = " WHERE ";
	// public final static String ORDER_BY_MARK = " ORDER BY ";
	// public final static String LIMIT_MARK = " LIMIT ";

	public final static String	FROM_MARK		= "FROM";
	public final static String	WHERE_MARK		= "WHERE";
	public final static String	ORDER_BY_MARK	= "ORDER BY";
	public final static String	LIMIT_MARK		= "LIMIT";

	public SqlVo parse(String sql) throws SqlParseException {
		sql = sql.trim();
		if (sql.endsWith(";")) {// fix bug
			sql = sql.substring(0, sql.length() - 1);
		}
		String ucSql = sql.toUpperCase();
		if (ucSql.startsWith(SELECT_MARK)) {
			return new SelectParser().parse(sql, ucSql);
		} else if (ucSql.startsWith(INSERT_MARK)) {
			return new InsertParser().parse(sql, ucSql);
		} else if (ucSql.startsWith(UPDATE_MARK)) {
			return new UpdateParser().parse(sql, ucSql);
		} else if (ucSql.startsWith(DELETE_MARK)) {
			return new DeleteParser().parse(sql, ucSql);
		}
		return null;
	}

	protected WhereCondition parseSelectWhere(String sql, int wherePos, int endPos) {
		// 1. a=b EqualCondition
		// 2. a>b MoreThanCondition
		// 3. a<b LessThanCondition
		// 4. a>=b GreaterEqualCondition
		// 5. a<=b LessThanEqualCondition
		// x. a<>b NotEqualCondition
		// 6. a like b LikeCondition
		// x. a in (1, 2, 3) InCondition
		// x. a not in (1, 2, 3) NotInCondition

		StringBuilder builder = new StringBuilder();
		String leftKey = null;

		LinkedList<BracketsCondition> stack = new LinkedList<BracketsCondition>();
		BracketsCondition bracketsCondition = new BracketsCondition(false);
		boolean isString = false; // 是否进入字符串采集
		for (int i = wherePos; i < endPos; i++) {
			char key = sql.charAt(i);
			switch (key) {
			case '(':
				if (isString) {
					builder.append(key);
				} else {
					if (builder.length() > 0) {
						throw new SqlParseException("Illegal where [(]: " + sql);
					}
					stack.push(bracketsCondition);
					BracketsCondition newBracketsCondition = new BracketsCondition(true);
					bracketsCondition.addCondition(newBracketsCondition);
					bracketsCondition = newBracketsCondition;
				}
				break;
			case ')':
				// if (builder.length() > 0) {
				// bracketsCondition.setValue(parseValueVo(builder.toString().trim(), false));
				// leftKey = null;
				// builder = new StringBuilder();
				// }
				// bracketsCondition = stack.pop();
				// break;

				// fix bug
				if (isString) {
					builder.append(key);
				} else {
					if (builder.length() > 0) {
						bracketsCondition.setValue(parseValueVo(builder.toString().trim(), false));
						leftKey = null;
						builder = new StringBuilder();
					}
					bracketsCondition = stack.pop();
				}
				break;
			case '=':
				if (isString) {
					builder.append(key);
				} else {
					if (builder.length() > 0) {
						leftKey = builder.toString();
						builder = new StringBuilder();
					}
					EqualCondition condition = new EqualCondition();
					condition.setName(leftKey);
					bracketsCondition.addCondition(condition);
				}
				break;
			case '!':
				if (isString) {
					builder.append(key);
					break;
				}
				if (((i + 1) < endPos) && ('=' == sql.charAt(i + 1))) {
					if (builder.length() > 0) {
						leftKey = builder.toString();
						builder = new StringBuilder();
					}
					NotEqualCondition condition = new NotEqualCondition();
					condition.setName(leftKey);
					bracketsCondition.addCondition(condition);
					i++;
					break;
				}
			case '>':
				if (isString) {
					builder.append(key);
				} else {
					if (builder.length() > 0) {
						leftKey = builder.toString();
						builder = new StringBuilder();
					}
					if ((i + 1) < endPos && '=' == sql.charAt(i + 1)) {
						GreaterEqualCondition condition = new GreaterEqualCondition();
						condition.setName(leftKey);
						bracketsCondition.addCondition(condition);
						i++;
					} else {
						MoreThanCondition condition = new MoreThanCondition();
						condition.setName(leftKey);
						bracketsCondition.addCondition(condition);
					}
				}
				break;
			case '<':
				if (isString) {
					builder.append(key);
				} else {
					if (builder.length() > 0) {
						leftKey = builder.toString();
						builder = new StringBuilder();
					}
					if ((i + 1) < endPos && '=' == sql.charAt(i + 1)) {
						LessThanEqualCondition condition = new LessThanEqualCondition();
						condition.setName(leftKey);
						bracketsCondition.addCondition(condition);
						i++;
					} else if ((i + 1) < endPos && '>' == sql.charAt(i + 1)) {
						NotEqualCondition condition = new NotEqualCondition();
						condition.setName(leftKey);
						bracketsCondition.addCondition(condition);
						i++;
					} else {
						LessThanCondition condition = new LessThanCondition();
						condition.setName(leftKey);
						bracketsCondition.addCondition(condition);
					}
				}
				break;
			case '\r':
			case '\n':
			case '\t':
			case ' ':
				if (isString) {
					builder.append(key);
				} else {
					if (builder.length() > 0) {
						String str = builder.toString();
						builder = new StringBuilder();
						if ("LIKE".equalsIgnoreCase(str)) {
							LikeCondition condition = new LikeCondition();
							condition.setName(leftKey);
							bracketsCondition.addCondition(condition);
						} else if ("IN".equalsIgnoreCase(str)) {
							InCondition condition = new InCondition();
							condition.setName(leftKey);
							bracketsCondition.addCondition(condition);
							i = findIn(bracketsCondition, sql, i, endPos);
						} else if ("NOT".equalsIgnoreCase(str)) {
							NotInCondition condition = new NotInCondition();
							condition.setName(leftKey);
							bracketsCondition.addCondition(condition);
							i = findNotIn(bracketsCondition, sql, i, endPos);
						} else if ("AND".equalsIgnoreCase(str)) {
							bracketsCondition.setAndOr(true);
							leftKey = null;
						} else if ("OR".equalsIgnoreCase(str)) {
							bracketsCondition.setAndOr(false);
							leftKey = null;
						} else {// left or rigth
							if (null == leftKey) {
								leftKey = str.trim();
							} else {
								bracketsCondition.setValue(parseValueVo(str.trim(), false));
								leftKey = null;
							}
						}
					}
				}
				break;
			case '\'':
				if (isString) {
					String str = builder.toString();
					builder = new StringBuilder();
					bracketsCondition.setValue(parseValueVo(str.trim(), true));
					leftKey = null;
					isString = false;
				} else {
					isString = true;
				}
				break;
			default:
				builder.append(key);
			}
		}

		if (builder.length() > 0) {
			String str = builder.toString();
			bracketsCondition.setValue(parseValueVo(str.trim(), false));
		}

		// TODO: 需要判断bracketsCondition是否合法

		return bracketsCondition;
	}

	protected int findIn(BracketsCondition condition, String sql, int startPos, int endPos) {
		// 可以直接找对末尾
		// x. a in (1, 2, 3) InCondition
		int startBracketsPos = sql.indexOf("(", startPos);
		if (-1 == startBracketsPos) {
			throw new SqlParseException("Illegal where in: " + sql);
		}
		// int endBracketsPos = sql.indexOf(")", startBracketsPos);
		// fix bug
		int endBracketsPos = findCharIndex(sql, startBracketsPos, ')');
		if (-1 == endBracketsPos) {
			throw new SqlParseException("Illegal where in: " + sql);
		}
		// INTEGER, DOUBLE, STRING
		// String[] array = sql.substring(startBracketsPos + 1, endBracketsPos).split(",");
		// fix bug
		String[] array = safeSplit(sql.substring(startBracketsPos + 1, endBracketsPos), ',');
		if (0 == array.length) {
			throw new SqlParseException("Illegal where in: " + sql);
		}

		List<ValueVo> value = new ArrayList<ValueVo>();
		for (int i = 0; i < array.length; i++) {
			value.add(parseValueVo(array[i].trim(), false));
		}
		condition.setValue(value);
		return endBracketsPos + 1;
	}

	protected int findNotIn(BracketsCondition condition, String sql, int startPos, int endPos) {
		return findIn(condition, sql, startPos, endPos);
	}

	private ValueVo parseValueVo(String val, boolean isString) {
		if (isString) {
			return new ValueVo(val, ValueType.STRING);
		}

		if (val.equalsIgnoreCase("null")) {
			return new ValueVo(null, ValueType.NULL);
		}

		if (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false")) {
			return new ValueVo(Boolean.parseBoolean(val), ValueType.BOOLEAN);
		}

		if ((val.startsWith("'") && val.endsWith("'")) || (val.startsWith("\"") && val.endsWith("\""))) {
			return new ValueVo(val.substring(1, val.length() - 1), ValueType.STRING);
		}

		if (isInteger(val)) {
			return new ValueVo(Integer.parseInt(val), ValueType.INTEGER);
		}
		return new ValueVo(Double.parseDouble(val), ValueType.DOUBLE);
	}

	protected ValueVo parseValueVo(String val) {
		return parseValueVo(val, false);
	}

	protected boolean isNumber(String var) {
		return var.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$");
	}

	protected boolean isInteger(String var) {
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
		return pattern.matcher(var).matches();
	}

	private boolean isLegalEmptyChar(char x) {
		if (x == ' ' || x == '\t' || x == '\r' || x == '\n' || x == '\f' || x == ' ') {
			return true;
		}
		return false;
	}

	protected int findWhere(String sql, int start) {
		// // public final static String WHERE_MARK = " WHERE ";
		char[] src = sql.toCharArray();
		int srcLength = src.length;
		boolean isString = false; // 是否进入字符串采集
		for (int i = 0; i < srcLength; i++) {
			char key = src[i];
			switch (key) {
			case '\'':
				isString = !isString;
				break;
			case 'W':
				if (!isString) {
					if ((i + WHERE_MARK.length() + 1) < srcLength) {
						String mark = new String(src, i, WHERE_MARK.length());
						if (mark.equals(WHERE_MARK) && isLegalEmptyChar(src[i - 1]) && isLegalEmptyChar(src[i + WHERE_MARK.length()])) {
							return i;
						}
					}
				}
				break;
			default:
				break;
			}
		}
		return -1;
	}

	protected int findFrom(String sql, int start) {
		char[] src = sql.toCharArray();
		int srcLength = src.length;
		boolean isString = false; // 是否进入字符串采集
		for (int i = 0; i < srcLength; i++) {
			char key = src[i];
			switch (key) {
			case '\'':
				isString = !isString;
				break;
			case 'F':
				if (!isString) {
					if ((i + FROM_MARK.length() + 1) < srcLength) {
						String mark = new String(src, i, FROM_MARK.length());
						if (mark.equals(FROM_MARK) && isLegalEmptyChar(src[i - 1]) && isLegalEmptyChar(src[i + FROM_MARK.length()])) {
							return i;
						}
					}
				}
				break;
			default:
				break;
			}
		}
		return -1;
	}

	protected int findLimit(String sql, int start) {
		char[] src = sql.toCharArray();
		int srcLength = src.length;
		boolean isString = false; // 是否进入字符串采集
		for (int i = 0; i < srcLength; i++) {
			char key = src[i];
			switch (key) {
			case '\'':
				isString = !isString;
				break;
			case 'L':
				if (!isString) {
					if ((i + LIMIT_MARK.length() + 1) < srcLength) {
						String mark = new String(src, i, LIMIT_MARK.length());
						if (mark.equals(LIMIT_MARK) && isLegalEmptyChar(src[i - 1]) && isLegalEmptyChar(src[i + LIMIT_MARK.length()])) {
							return i;
						}
					}
				}
				break;
			default:
				break;
			}
		}
		return -1;
	}

	protected int findOrderBy(String sql, int start) {
		// public final static String ORDER_BY_MARK = " ORDER BY ";
		char[] src = sql.toCharArray();
		int srcLength = src.length;
		boolean isString = false; // 是否进入字符串采集
		for (int i = 0; i < srcLength; i++) {
			char key = src[i];
			switch (key) {
			case '\'':
				isString = !isString;
				break;
			case 'O':
				if (!isString) {
					if ((i + ORDER_BY_MARK.length() + 1) < srcLength) {
						String mark = new String(src, i, ORDER_BY_MARK.length());
						if (mark.equals(ORDER_BY_MARK) && isLegalEmptyChar(src[i - 1]) && isLegalEmptyChar(src[i + ORDER_BY_MARK.length()])) {
							return i;
						}
					}
				}
				break;
			default:
				break;
			}
		}
		return -1;
	}

	/** 把字符串分割成数组 */
	protected String[] safeSplit(String src, char separator) {
		List<String> temp = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		boolean isString = false; // 是否进入字符串采集
		for (int i = 0; i < src.length(); i++) {
			char key = src.charAt(i);
			switch (key) {
			case '\'':
				isString = !isString;
				sb.append(key);
				break;
			default:
				if (separator == key && !isString) {
					if (sb.length() > 0) {
						temp.add(sb.toString());
						sb = new StringBuilder();
					}
				} else {
					sb.append(key);
				}
				break;
			}
		}

		if (sb.length() > 0) {
			temp.add(sb.toString());
		}

		String[] result = new String[temp.size()];
		return temp.toArray(result);
	}

	// protected int findCharIndex(String src, char chr) {
	// boolean isString = false; // 是否进入字符串采集
	// for (int i = 0; i < src.length(); i++) {
	// char key = src.charAt(i);
	// switch (key) {
	// case '\'':
	// isString = !isString;
	// break;
	// default:
	// if (chr == key && !isString) {
	// return i;
	// }
	// break;
	// }
	// }
	// return -1;
	// }

	protected int findCharIndex(String src, char chr) {
		return findCharIndex(src, 0, src.length(), chr);
	}

	protected int findCharIndex(String src, int start, char chr) {
		return findCharIndex(src, start, src.length(), chr);
	}

	protected int findCharIndex(String src, int start, int end, char chr) {
		boolean isString = false; // 是否进入字符串采集
		for (int i = start; i < end; i++) {
			char key = src.charAt(i);
			switch (key) {
			case '\'':
				isString = !isString;
				break;
			default:
				if (chr == key && !isString) {
					return i;
				}
				break;
			}
		}
		return -1;
	}

	public static void main(String[] args) {
		// String sql = "select count(*) from table";
		// String sql = "select a, b from table where a>2 or (c = '4' and c = 1) order by a b,c DESC limit 1,2";
		String sql = "select * from table where ((c= '4' or c =1) or (c = '4' and c = 1)) and (c = '4' and ( c = '4' and c = 1)) order by a b asc,c limit 1";
		// String sql = "INSERT INTO tbTrade (name, age) VALUES ('gaop34', 22)";
		// String sql = "DELETE FROM 表名称 WHERE age = 22";
		// String sql = " UPDATE Person SET FirstName = FirstName +4, WHERE LastName = 'Wilson'";
		SqlParser parser = new SqlParser();
		SqlVo sqlVo = parser.parse(sql);
		System.out.println(sqlVo.toSQL());

	}
}
