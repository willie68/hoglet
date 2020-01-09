package de.mcs.utils;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class JsonByteArraySerializer implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {

  @Override
  public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
    return new JsonPrimitive(ByteArrayUtils.bytesAsHexString(src));
  }

  @Override
  public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    try {
      return ByteArrayUtils.decodeHex(json.getAsJsonPrimitive().getAsString());
    } catch (Exception e) {
      throw new JsonParseException(e);
    }
  }

}
